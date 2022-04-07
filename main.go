package main

import (
	"chukcha/integration"
	"flag"
	"log"
)

var (
	dirname = flag.String("dirname", "", "The dirname where to put all the data")
	port    = flag.Uint("port", 8061, "Network port to listen on")
)

func main() {
	flag.Parse()

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if err := integration.InitAndServe(*dirname, *port); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
