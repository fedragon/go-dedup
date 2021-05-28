package internal

import "time"

type Media struct {
	Path      string
	Hash      []byte
	Timestamp time.Time
	Err       error
}
