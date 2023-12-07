package main

import (
	"log"
	"net/url"
	"os"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
)

func main() {
	var err error

	// create an in-memory test server
	o := test.DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.LeafNode.Host = o.Host
	o.LeafNode.Port = server.DEFAULT_LEAFNODE_PORT
	o.NoSystemAccount = true

	if o.StoreDir, err = os.MkdirTemp("bi-dir-sync-hub", "test"); err != nil {
		log.Fatalf("failed to create temporary jetstream directory: %v", err)
	}

	srv := test.RunServer(&o)
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		// remove jetstream state
		_ = os.RemoveAll(o.StoreDir)
	}()

	ol := test.DefaultTestOptions
	o.Port = -2
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{
		{
			URLs: []*url.URL{},
		},
	}

	if ol.StoreDir, err = os.MkdirTemp("bi-dir-sync-leaf", "test"); err != nil {
		log.Fatalf("failed to create temporary jetstream directory: %v", err)
	}

	conf := `
  port: -2
  leaf {
   remotes = [
    {
     url: "leafnode://127.0.0.1"
    }
   ]
  }
 `

	err = os.WriteFile("conf", []byte(conf), 0644)
	if err != nil {
		log.Fatal("Error creating temp file: ", err)
	}

	srvLeaf := test.RunServer(&ol)

	defer func() {
		srvLeaf.Shutdown()
		srvLeaf.WaitForShutdown()
		// remove jetstream state
		_ = os.RemoveAll(ol.StoreDir)
	}()

}
