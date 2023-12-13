package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
)

func checkFor(totalWait, sleepDur time.Duration, f func() error) {
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	var err error

	// create an in-memory test server
	o := test.DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.LeafNode.Host = o.Host
	o.NoSystemAccount = true
	o.LeafNode.Port = server.DEFAULT_LEAFNODE_PORT

	if o.StoreDir, err = os.MkdirTemp("bi-directional-sync", "hub"); err != nil {
		log.Fatalf("failed to create temporary jetstream directory: %v", err)
	}

	srv := test.RunServer(&o)
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		// remove jetstream state
		_ = os.RemoveAll(o.StoreDir)
	}()

	u, err := url.Parse("leafnode://127.0.0.1")
	if err != nil {
		log.Fatal("Error parsing URL: ", err)
	}

	ol := test.DefaultTestOptions
	ol.Port = -1
	ol.LeafNode.Remotes = []*server.RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
		},
	}

	if ol.StoreDir, err = os.MkdirTemp("bi-directional-sync", "leaf"); err != nil {
		log.Fatalf("failed to create temporary jetstream directory: %v", err)
	}

	srvLeaf := test.RunServer(&ol)

	defer func() {
		srvLeaf.Shutdown()
		srvLeaf.WaitForShutdown()
		// remove jetstream state
		_ = os.RemoveAll(ol.StoreDir)
	}()

	checkFor(5*time.Second, 100*time.Millisecond, func() error {
		nln := srv.NumLeafNodes()
		if nln != 1 {
			return fmt.Errorf("Expected 1 leaf node, got: %v", nln)
		}

		return nil
	})
}
