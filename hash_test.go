package main

import (
	"os"
	"reflect"
	"testing"
)

func TestHash(t *testing.T) {
	workdir, err := os.Getwd()
	if err != nil {
		t.Fatalf(err.Error())
	}

	cases := []struct {
		name     string
		pathA    string
		pathB    string
		expected bool
	}{
		{
			name:     "hashing the same file twice returns the same value",
			pathA:    workdir + "/test/data/doge.jpg",
			pathB:    workdir + "/test/data/doge.jpg",
			expected: true,
		},
		{
			name:     "hashing two files with same content but different name returns the same value",
			pathA:    workdir + "/test/data/doge.jpg",
			pathB:    workdir + "/test/data/same-doge.jpg",
			expected: true,
		},
		{
			name:     "hashing two different files returns different values",
			pathA:    workdir + "/test/data/doge.jpg",
			pathB:    workdir + "/test/data/grumpy-cat.jpg",
			expected: false,
		},
	}

	for _, c := range cases {
		a, err := hash(c.pathA)
		if err != nil {
			t.Errorf(err.Error())
		}
		b, err := hash(c.pathB)
		if err != nil {
			t.Errorf(err.Error())
		}

		equal := reflect.DeepEqual(a, b)
		if equal != c.expected {
			t.Errorf("%v\n\tExpected %v but got %v instead", c.name, c.expected, equal)
		}
	}
}

func BenchmarkHash(b *testing.B) {
	workdir, err := os.Getwd()
	if err != nil {
		b.Fatalf(err.Error())
	}

	for i := 0; i < b.N; i++ {
		_, err := hash(workdir + "/test/data/doge.jpg")
		if err != nil {
			b.Errorf(err.Error())
		}
	}
}
