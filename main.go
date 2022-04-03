package main

import (
	"chukcha/server"
	"chukcha/web"
	"log"
)

func main() {
	s := web.NewServer(&server.InMemory{})
	log.Printf("Listening connections")
	s.Serve()
}
