package fs

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fedragon/go-dedup/internal/models"

	"lukechampine.com/blake3"
)

func hash(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := blake3.New(256, nil)
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func Walk(root string, fileTypes []string) <-chan models.Media {
	media := make(chan models.Media)

	go func() {
		defer close(media)

		typesMap := make(map[string]struct{})
		for _, t := range fileTypes {
			typesMap[t] = struct{}{}
		}

		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if !d.IsDir() {
				ext := strings.ToLower(filepath.Ext(d.Name()))
				if _, exists := typesMap[ext]; exists {
					bytes, err := hash(path)
					if err != nil {
						return err
					}

					media <- models.Media{
						Path:      path,
						Hash:      bytes,
						Timestamp: time.Now(),
					}
				}
			}

			return nil
		})
		if err != nil {
			media <- models.Media{Err: err}
		}
	}()

	return media
}
