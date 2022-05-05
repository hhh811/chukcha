package main

import (
	"chukcha/integration"
	"flag"
	"log"
	"os"
	"strings"
)

var (
	clusterName  = flag.String("cluster", "default", "The name of the cluster (must specify if sharing a single etcd instance with several Chukcha instances)")
	instanceName = flag.String("instance", "", "The unique instance name")
	dirname      = flag.String("dirname", "", "The dirname where to put all the data")
	listenAddr   = flag.String("listen", "127.0.0.1:8061", "Network port to listen on")
	etcdAddr     = flag.String("etcd", "http://127.0.0.1:2379", "The network address of etcd server(s)")
)

func main() {
	flag.Parse()

	if *clusterName == "" {
		log.Fatalf("The flag `--cluster` must not be empty")
	}

	if *instanceName == "" {
		log.Fatalf("The flag `--instanc` must be provided")
	}

	if *dirname == "" {
		log.Fatalf("The flag `--dirname` must be provided")
	}

	if *etcdAddr == "" {
		log.Fatalf("The flat `--etcd` must be provided")
	}

	a := integration.InitArgs{
		LogWriter:    os.Stderr,
		EtcdAddr:     strings.Split(*etcdAddr, ","),
		ClusterName:  *clusterName,
		InstanceName: *instanceName,
		DirName:      *dirname,
		ListenAddr:   *listenAddr,
		MaxChunkSize: 20 * 1024 * 1024,
	}

	if err := integration.InitAndServe(a); err != nil {
		log.Fatalf("InitAndServe failed: %v", err)
	}
}
