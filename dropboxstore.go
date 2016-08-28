package dropboxstore

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/tolgaakyuz/go-dropbox"
	"github.com/tus/tusd"
)

var defaultFilePerm = os.FileMode(0664)

// Config holds main config
type Config struct {
	// Token for the dropbox client
	Token string

	// ChunkSize is the number of bytes
	// to be uploaded
	ChunkSize int64

	// Path for storing .info file
	Path string
}

// DropboxStore is the store
type DropboxStore struct {
	Config *Config
	svc    *dropbox.Client
}

// New to create new store
func New(config *Config) *DropboxStore {
	store := &DropboxStore{Config: config}

	if config.Token != "" {
		store.svc = dropbox.New(dropbox.NewConfig(config.Token))
	}

	return store
}

// UseIn decides which events this store will be used in
func (s DropboxStore) UseIn(composer *tusd.StoreComposer) {
	composer.UseCore(s)
	composer.UseTerminater(s)
	composer.UseFinisher(s)
}

// NewUpload initilazes a new upload request
func (s DropboxStore) NewUpload(info tusd.FileInfo) (id string, err error) {
	s.svc = dropbox.New(dropbox.NewConfig(info.MetaData["token"]))

	// write a zero-valued byte slice to get a session id
	start, err := s.svc.Files.Upload.Session.Start(&dropbox.SessionStartInput{
		Reader: bytes.NewReader(make([]byte, 0)),
	})
	if err != nil {
		return
	}

	id = start.SessionCursor.ID
	info.ID = id
	s.writeInfo(id, info)

	return
}

// WriteChunk writes the given chunk to dropbox
func (s DropboxStore) WriteChunk(id string, offset int64, src io.Reader) (bytesUploaded int64, err error) {
	info, err := s.getInfo(id)
	if err != nil {
		return
	}

	s.svc = dropbox.New(dropbox.NewConfig(info.MetaData["token"]))

	lr := &io.LimitedReader{
		R: src,
		N: s.Config.ChunkSize,
	}

uploadparts:
	for {
		lr.N = s.Config.ChunkSize

		err = s.svc.Files.Upload.Session.Append(&dropbox.SessionAppendInput{
			Cursor: dropbox.SessionCursor{
				ID:     id,
				Offset: offset,
			},
			Reader: lr,
		})
		if err != nil {
			return
		}

		uploaded := int64(s.Config.ChunkSize - lr.N)
		offset += uploaded
		bytesUploaded += uploaded

		if uploaded == 0 {
			break uploadparts
		}
	}

	info.ID = id
	info.Offset += bytesUploaded
	s.writeInfo(id, info)

	return
}

// GetInfo returns the file info
func (s DropboxStore) GetInfo(id string) (info tusd.FileInfo, err error) {
	info, err = s.getInfo(id)
	delete(info.MetaData, "token")
	return
}

// Terminate the upload, clear .info file
func (s DropboxStore) Terminate(id string) (err error) {
	err = os.Remove(s.infoPath(id))
	return
}

// FinishUpload to finish the upload
func (s DropboxStore) FinishUpload(id string) (err error) {
	info, err := s.getInfo(id)
	if err != nil {
		return
	}

	s.svc = dropbox.New(dropbox.NewConfig(info.MetaData["token"]))

	_, err = s.svc.Files.Upload.Session.Finish(&dropbox.SessionFinishInput{
		Cursor: dropbox.SessionCursor{
			ID:     id,
			Offset: info.Offset,
		},
		Commit: &dropbox.UploadInput{
			Path:           info.MetaData["path"],
			Mode:           dropbox.WriteModeOverwrite,
			Mute:           false,
			ClientModified: dropbox.JSONTime(time.Now()),
			Reader:         bytes.NewReader(make([]byte, 0)),
		},
	})

	return
}

func (s DropboxStore) getInfo(id string) (info tusd.FileInfo, err error) {
	data, err := ioutil.ReadFile(s.infoPath(id))
	if err != nil {
		return
	}

	err = json.Unmarshal(data, &info)
	return
}

func (s DropboxStore) writeInfo(id string, info tusd.FileInfo) (err error) {
	data, err := json.Marshal(info)
	if err != nil {
		return
	}

	err = ioutil.WriteFile(s.infoPath(id), data, defaultFilePerm)
	return
}

func (s DropboxStore) infoPath(id string) string {
	return s.Config.Path + "/" + id + ".info"
}
