package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"strconv"
	"time"
)

var (
	fCommandFile     string
	fKafkaConfigFile string
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
	r.AppName = value.Id.AppName
	r.Path = value.Id.Path
	r.Kind = value.Id.Kind
	r.Unit = value.Id.Unit
	var err error
	switch r.Kind {
	case types.Bool:
		if value.Value == "true" {
			r.Value = true
		}
		r.Value = false
	case types.Int:
		if r.Value, err = strconv.ParseInt(value.Value, 10, 64); err != nil {
			panic(err)
		}
	case types.Uint:
		if r.Value, err = strconv.ParseUint(value.Value, 10, 64); err != nil {
			panic(err)
		}
	case types.Float:
		if r.Value, err = strconv.ParseFloat(value.Value, 64); err != nil {
			panic(err)
		}
	case types.String:
		r.Value = value.Value
	case types.GoTime:
		if r.Value, err = time.Parse("01/02/2006 15:04:05", value.Value); err != nil {
			panic(err)
		}
	default:
		panic("Unrecognized type")

	}
	r.Timestamp = messages.TimeToFloat(timestamp)
}

func playCommands(
	values []metricValue, timestamp time.Time, writer pstore.Writer) {
	records := make([]pstore.Record, len(values))
	for i := range values {
		initRecord(&values[i], timestamp, &records[i])
	}
	if err := writer.Write(records); err != nil {
		log.Println("Error writing: ", err)
	}
}

func readCommands(commands *[]metricCommand) {
	f, err := os.Open(fCommandFile)
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

type dumbWriter struct {
}

func (d dumbWriter) IsTypeSupported(t types.Type) bool { return true }

func (d dumbWriter) Write(values []pstore.Record) error {
	for i := range values {
		fmt.Println(values[i])
	}
	fmt.Println()
	return nil
}

func createKafkaWriter() (result pstore.Writer) {
	if fKafkaConfigFile == "" {
		return dumbWriter{}
	}
	f, err := os.Open(fKafkaConfigFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var config kafka.Config
	if err = config.Read(f); err != nil {
		panic(err)
	}
	if result, err = kafka.NewWriter(&config); err != nil {
		panic(err)
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

func init() {
	flag.StringVar(
		&fKafkaConfigFile,
		"kafka_config_file",
		"",
		"kafka configuration file")
	flag.StringVar(
		&fCommandFile,
		"command_file",
		"",
		"command file")
}
