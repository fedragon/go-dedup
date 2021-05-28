package main

import (
	"crypto/sha256"
	"io"
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

type Media struct {
	Path      string
	Hash      []byte
	Timestamp time.Time
	Err       error
}

func main() {
	media := walk(os.Getenv("ROOT"))
	var count int

	for m := range media {
		if count%1000 == 0 {
			log.Printf("Found %v media so far\n", count)
		}

		if m.Err != nil {
			log.Fatalf(m.Err.Error())
		} else {
			count++
		}
	}

	log.Printf("Found %v media\n", count)
}

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

func walk(root string) <-chan Media {
	media := make(chan Media)

	go func() {
		defer close(media)

		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			ext := strings.ToLower(filepath.Ext(info.Name()))
			if ext == JPG || ext == JPEG || ext == MP4 || ext == ORF {
				bytes, err := hash(path)
				if err != nil {
					return err
				}

				media <- Media{
					Path:      path,
					Hash:      bytes,
					Timestamp: time.Now(),
				}
			}

			return nil
		})

		if err != nil {
			media <- Media{Err: err}
		}
	}()

	return media
}
