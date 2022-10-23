package core

import (
	"context"
	"sync"
)

func merge(ctx context.Context, channels ...<-chan int64) <-chan int64 {
	var wg sync.WaitGroup

	wg.Add(len(channels))
	media := make(chan int64)
	multiplex := func(c <-chan int64) {
		defer wg.Done()
		for i := range c {
			select {
			case <-ctx.Done():
				return
			case media <- i:
			}
		}
	}

	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(media)
	}()

	return media
}
