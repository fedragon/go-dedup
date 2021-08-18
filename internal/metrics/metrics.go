package metrics

import (
	"time"

	"github.com/netdata/go-statsd"
	log "github.com/sirupsen/logrus"
)

type Metrics struct {
	client *statsd.Client
}

func NewMetrics() *Metrics {
	statsWriter, err := statsd.UDP(":8125")
	if err != nil {
		log.Fatalf(err.Error())
	}

	client := statsd.NewClient(statsWriter, "go-dedup.")
	client.FlushEvery(3 * time.Second)

	return &Metrics{client}
}

func NoMetrics() *Metrics {
	return &Metrics{}
}

func (x *Metrics) Close() error {
	return x.client.Close()
}

func (x *Metrics) Record(metricName string) func() error {
	if x.client != nil {
		return x.client.Record(metricName, 1)
	}

	return func() error { return nil }
}

func (x *Metrics) Increment(metricName string) error {
	if x.client != nil {
		return x.client.Increment(metricName)
	}

	return nil
}
