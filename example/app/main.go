package main

import (
	"chukcha/client"
	"log"
	"net/http"
)

type app struct {
	cl *client.Simple
}

func main() {
	a := &app{
		cl: client.NewSimple([]string{"http://127.0.0.1:8061", "http://127.0.0.1:8062"}),
	}

	http.HandleFunc("/hello", a.helloHandler)
	http.HandleFunc("/", a.indexHandler)

	go CollectEventsThread(a.cl, "http://127.0.0.1:8123/", "/tmp/chukcha-client-state.txt")

	log.Printf("Running")
	log.Fatal(http.ListenAndServe(":80", nil))
}
