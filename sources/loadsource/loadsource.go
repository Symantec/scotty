package loadsource

import (
	"fmt"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sort"
	"time"
)

type connectorType struct {
	config *Config
}

func (c connectorType) Connect(
	host string, port uint, config sources.Config) (sources.Poller, error) {
	return &pollerType{c.config}, nil
}

func (c connectorType) Name() string {
	return "load test"
}

type pollerType struct {
	config *Config
}

func (p *pollerType) Poll() (metrics.List, error) {
	now := time.Now()
	result := make(metrics.SimpleList, p.config.Count)
	for i := range result {
		result[i].Path = fmt.Sprintf("/metric/%d", i)
		result[i].Description = "A test metric"
		result[i].Unit = units.None
		result[i].Value = generateValue(now, i+1)
		result[i].TimeStamp = now
		result[i].GroupId = i / 10
	}
	sort.Sort(result)
	return result, nil
}

func (p *pollerType) Close() error {
	return nil
}

func toSawTooth(t time.Time) float64 {
	res := float64(t.Unix()%900) + float64(t.Nanosecond())/1000.0/1000.0/1000.0
	return res / 900.0
}

func generateValue(t time.Time, id int) float64 {
	return toSawTooth(t) * float64(id)
}
