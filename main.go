package main

import (
	"chukcha/server"
	"chukcha/web"
	"flag"
	"log"
	"os"
	"path/filepath"
)

var (
	dirname = flag.String("dirname", "", "The dirname where to put all the data")
	port    = flag.Uint("port", 8061, "Network port to listen on")
)

func main() {
	flag.Parse()

	var backend web.Storage

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	filename := filepath.Join(*dirname, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("Could not create test file %q: %v", filename, err)
	}
	defer fp.Close()
	os.Remove(fp.Name())

	backend, err = server.NewOndisk(*dirname)
	if err != nil {
		log.Fatalf("Cound not initialize on-disk backend: %v", err)
	}

	s := web.NewServer(backend, *port)
	log.Printf("Listening connections")
	s.Serve()
}
