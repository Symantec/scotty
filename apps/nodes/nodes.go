package main

import (
	"fmt"
	"github.com/Symantec/scotty/nodes"
	"log"
)

func main() {
	names, err := nodes.GetByCluster("ash1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(names))
}
