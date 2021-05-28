package main

import (
	"os"
	"testing"
)

func TestWalk(t *testing.T) {
	workdir, err := os.Getwd()
	if err != nil {
		t.Fatalf(err.Error())
	}

	cases := []struct {
		name     string
		root     string
		expected int
	}{
		{
			name:     "walk returns all images in a directory",
			root:     workdir + "/test/data",
			expected: 3,
		},
		{
			name:     "walk returns all images in a directory and all its subdirectories",
			root:     workdir,
			expected: 3,
		},
	}

	for _, c := range cases {
		var count int
		for i := range walk(c.root) {
			if i.Err != nil {
				t.Errorf(i.Err.Error())
			}

			count++
		}

		if count != c.expected {
			t.Errorf("%v\n\tExpected %v but got %v instead", c.name, c.expected, count)
		}
	}
}
