package internal

import "time"

type Media struct {
	Hash      []byte `json:"-"`
	Path      string
	Timestamp time.Time
	Err       error `json:"-"`
}

type AggregatedMedia struct {
	Hash   []byte
	Medias []Media
}
