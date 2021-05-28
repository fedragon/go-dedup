package app

import (
	"crypto/sha256"
	"github.com/fedragon/go-dedup/internal"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	JPG  = ".jpg"
	JPEG = ".jpeg"
	MP4  = ".mp4"
	ORF  = ".orf"
)

func hash(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func Walk(root string) <-chan internal.Media {
	media := make(chan internal.Media)

	go func() {
		defer close(media)

		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			ext := strings.ToLower(filepath.Ext(d.Name()))
			if ext == JPG || ext == JPEG || ext == MP4 || ext == ORF {
				bytes, err := hash(path)
				if err != nil {
					return err
				}

				media <- internal.Media{
					Path:      path,
					Hash:      bytes,
					Timestamp: time.Now(),
				}
			}

			return nil
		})

		if err != nil {
			media <- internal.Media{Err: err}
		}
	}()

	return media
}
