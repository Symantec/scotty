package main

import (
	"bytes"
	"flag"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/influx"
	"github.com/Symantec/scotty/pstore/config/kafka"
	"github.com/Symantec/scotty/pstore/config/tsdb"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"strconv"
	"time"
)

var (
	fKafkaConfigFile = flag.String(
		"kafka_config_file",
		"",
		"kafka configuration file")
	fInfluxConfigFile = flag.String(
		"influx_config_file",
		"",
		"influx configuration file")
	fTsdbConfigFile = flag.String(
		"tsdb_config_file",
		"",
		"tsdb configuration file")
	fCommandFile = flag.String(
		"command_file",
		"",
		"command file")
)

type metricMetaData struct {
	HostName string
	AppName  string
	Path     string
	Kind     types.Type
	Unit     units.Unit
}

type metricValue struct {
	Id    *metricMetaData
	Value string
}

type metricCommand struct {
	Values []metricValue
	Sleep  uint
}

func initRecord(value *metricValue, timestamp time.Time, r *pstore.Record) {
	r.HostName = value.Id.HostName
	r.Tags = pstore.TagGroup{pstore.TagAppName: value.Id.AppName}
	r.Path = value.Id.Path
	r.Kind = value.Id.Kind
	r.Unit = value.Id.Unit
	var err error
	switch r.Kind {
	case types.Bool:
		if value.Value == "true" {
			r.Value = true
		} else {
			r.Value = false
		}
	case types.Int8:
		var intVal int64
		if intVal, err = strconv.ParseInt(value.Value, 10, 64); err != nil {
			panic(err)
		}
		r.Value = int8(intVal)
	case types.Int16:
		var intVal int64
		if intVal, err = strconv.ParseInt(value.Value, 10, 64); err != nil {
			panic(err)
		}
		r.Value = int16(intVal)
	case types.Int32:
		var intVal int64
		if intVal, err = strconv.ParseInt(value.Value, 10, 64); err != nil {
			panic(err)
		}
		r.Value = int32(intVal)
	case types.Int64:
		if r.Value, err = strconv.ParseInt(value.Value, 10, 64); err != nil {
			panic(err)
		}
	case types.Uint8:
		var uintVal uint64
		if uintVal, err = strconv.ParseUint(value.Value, 10, 64); err != nil {
			panic(err)
		}
		r.Value = uint8(uintVal)
	case types.Uint16:
		var uintVal uint64
		if uintVal, err = strconv.ParseUint(value.Value, 10, 64); err != nil {
			panic(err)
		}
		r.Value = uint16(uintVal)
	case types.Uint32:
		var uintVal uint64
		if uintVal, err = strconv.ParseUint(value.Value, 10, 64); err != nil {
			panic(err)
		}
		r.Value = uint32(uintVal)
	case types.Uint64:
		if r.Value, err = strconv.ParseUint(value.Value, 10, 64); err != nil {
			panic(err)
		}
	case types.Float32:
		var floatVal float64
		if floatVal, err = strconv.ParseFloat(value.Value, 64); err != nil {
			panic(err)
		}
		r.Value = float32(floatVal)
	case types.Float64:
		if r.Value, err = strconv.ParseFloat(value.Value, 64); err != nil {
			panic(err)
		}
	case types.String:
		r.Value = value.Value
	case types.GoTime:
		if r.Value, err = time.Parse("01/02/2006 15:04:05", value.Value); err != nil {
			panic(err)
		}
	case types.GoDuration:
		if r.Value, err = time.ParseDuration(value.Value); err != nil {
			panic(err)
		}
	default:
		panic("Unrecognized type")

	}
	r.Timestamp = timestamp
}

func playCommands(
	values []metricValue, timestamp time.Time, writer pstore.RecordWriter) {
	records := make([]pstore.Record, len(values))
	for i := range values {
		initRecord(&values[i], timestamp, &records[i])
	}
	if err := writer.Write(records); err != nil {
		log.Println("Error writing: ", err)
	}
}

func readCommands(commands *[]metricCommand) {
	f, err := os.Open(*fCommandFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var content bytes.Buffer
	if _, err = content.ReadFrom(f); err != nil {
		panic(err)
	}
	if err = yaml.Unmarshal(content.Bytes(), commands); err != nil {
		panic(err)
	}
}

func createKafkaWriter() (result pstore.RecordWriter) {
	var err error
	if *fKafkaConfigFile != "" {
		result, err = kafka.FromFile(*fKafkaConfigFile)
	} else if *fInfluxConfigFile != "" {
		result, err = influx.FromFile(*fInfluxConfigFile)
	} else if *fTsdbConfigFile != "" {
		result, err = tsdb.FromFile(*fTsdbConfigFile)
	} else {
		return kafka.NewFakeWriter()
	}
	if err != nil {
		log.Fatal(err)
	}
	return
}

func main() {
	flag.Parse()
	var commands []metricCommand
	writer := createKafkaWriter()
	readCommands(&commands)
	if len(commands) == 0 {
		log.Fatal("No commands, nothing to do.")
	}
	timestamp := time.Now()
	idx := 0
	for {
		playCommands(commands[idx].Values, timestamp, writer)
		if commands[idx].Sleep <= 0 {
			panic("Need a positive time delay.")
		}
		sleepTime := time.Duration(commands[idx].Sleep) * time.Second
		time.Sleep(sleepTime)
		timestamp = timestamp.Add(sleepTime)
		idx++
		if idx == len(commands) {
			idx = 0
		}
	}
}
