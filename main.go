package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ORF  = ".orf"
	JPG  = ".jpg"
	JPEG = ".jpeg"
)

type Image struct {
	Path      string
	Hash      []byte
	Timestamp time.Time
	Err       error
}

func main() {
	images := walk(os.Getenv("ROOT"))
	var count int

	for i := range images {
		if i.Err != nil {
			log.Printf(i.Err.Error())
		} else {
			count++
		}
	}

	fmt.Printf("Found %v images\n", count)
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

func walk(root string) <-chan Image {
	images := make(chan Image)

	go func() {
		defer close(images)

		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			ext := strings.ToLower(filepath.Ext(info.Name()))
			if ext == JPG || ext == JPEG || ext == ORF {
				bytes, err := hash(path)
				if err != nil {
					return err
				}

				images <- Image{
					Path:      path,
					Hash:      bytes,
					Timestamp: time.Now(),
				}
			}

			return nil
		})

		if err != nil {
			images <- Image{Err: err}
		}
	}()

	return images
}
