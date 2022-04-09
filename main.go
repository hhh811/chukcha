package main

import (
	"chukcha/integration"
	"flag"
	"log"
)

var (
	instanceName = flag.String("instance-name", "", "The unique instance name")
	dirname      = flag.String("dirname", "", "The dirname where to put all the data")
	listenAddr   = flag.String("listen", "127.0.0.1:8061", "Network port to listen on")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")
)

func main() {
	flag.Parse()

	if *instanceName == "" {
		log.Fatalf("The flag `--instanceName` must be provided")
	}

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if *etcdAddr == "" {
		log.Fatalf("The flat `--etcd` must be provided")
	}

	if err := integration.InitAndServe(*etcdAddr, *instanceName, *dirname, *listenAddr); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
