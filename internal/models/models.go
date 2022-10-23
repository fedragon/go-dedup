package models

import "time"

type Media struct {
	Hash      []byte
	Path      string
	Timestamp time.Time
	Err       error `json:"-"`
}

type AggregatedMedia struct {
	Hash  []byte
	Paths []string
}
