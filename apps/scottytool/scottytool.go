package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/Symantec/scotty/consul"
	"io"
	"log"
	"os"
)

const (
	kGeneralUsage = `
Usage: scottytool subcommand flags

sub commands:
  upload: uploads a config file
  list: lists a config file
`
)

func doUpload(args []string) {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	fileName := fs.String("file", "", "path of file to upload")
	inline := fs.Bool(
		"stdin", false, "Read file contents from stdin")
	fs.Parse(args)
	var intake io.Reader
	if *inline {
		intake = os.Stdin
	} else {
		if *fileName == "" {
			log.Fatal("scottytool upload: Either --stdin or --file must be present.")
		}
		file, err := os.Open(*fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		intake = file
	}
	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, intake); err != nil {
		log.Fatal(err)
	}
	coord, err := consul.GetCoordinator(nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := coord.PutPStoreConfig(buffer.String()); err != nil {
		log.Fatal(err)
	}
}

func doList(args []string) {
	coord, err := consul.GetCoordinator(nil)
	if err != nil {
		log.Fatal(err)
	}
	contents, err := coord.GetPStoreConfig()
	if err == consul.ErrMissing {
		// Do nothing special.
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Print(contents)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal(kGeneralUsage)
	}
	subCommand := os.Args[1]
	args := os.Args[2:]
	switch subCommand {
	case "upload":
		doUpload(args)
	case "list":
		doList(args)
	default:
		log.Fatal(kGeneralUsage)
	}
}
