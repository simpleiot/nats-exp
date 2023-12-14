package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	var err error

	// start hub instance
	srv, srvS, err := hubInit()
	if err != nil {
		log.Fatal("Error init hub server: ", err)
	}

	defer cleanup(srv, srvS)

	// start leaf instance
	srvLeaf, srvLeafS, err := leafInit()
	if err != nil {
		log.Fatal("Error init leaf server: ", err)
	}

	defer cleanup(srvLeaf, srvLeafS)

	// make sure leaf node is connected
	checkFor(5*time.Second, 100*time.Millisecond, func() error {
		nln := srv.NumLeafNodes()
		if nln != 1 {
			return fmt.Errorf("Expected 1 leaf node, got: %v", nln)
		}

		return nil
	})

	err = hubCreateStream(srv)
	if err != nil {
		fmt.Printf("Error creating hub stream: %v\n", err)
		return
	}

	err = leafConnectHubStream(srvLeaf)
	if err != nil {
		fmt.Printf("Error leaf connecting to hub stream: %v\n", err)
		return
	}

	err = leafSourceHubStream(srvLeaf)
	if err != nil {
		fmt.Printf("Error leaf sourcing hub stream: %v\n", err)
		return
	}

}

func hubInit() (*server.Server, string, error) {
	var err error

	o := test.DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.JetStreamDomain = "hub"
	o.LeafNode.Host = o.Host
	o.NoSystemAccount = true
	o.LeafNode.Port = server.DEFAULT_LEAFNODE_PORT

	if o.StoreDir, err = os.MkdirTemp("bi-directional-sync", "hub"); err != nil {
		return nil, "", fmt.Errorf("failed to create temporary jetstream directory: %v", err)
	}

	return test.RunServer(&o), o.StoreDir, nil
}

func leafInit() (*server.Server, string, error) {
	u, err := url.Parse("leafnode://127.0.0.1")
	if err != nil {
		log.Fatal("Error parsing URL: ", err)
	}

	ol := test.DefaultTestOptions
	ol.Port = -1
	ol.JetStream = true
	ol.LeafNode.Remotes = []*server.RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
		},
	}

	if ol.StoreDir, err = os.MkdirTemp("bi-directional-sync", "leaf"); err != nil {
		log.Fatalf("failed to create temporary jetstream directory: %v", err)
	}

	return test.RunServer(&ol), ol.StoreDir, nil
}

func cleanup(srv *server.Server, storeDir string) {
	srv.Shutdown()
	srv.WaitForShutdown()
	_ = os.RemoveAll(storeDir)
}

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

func hubCreateStream(srv *server.Server) error {
	url := srv.ClientURL()

	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.NewWithDomain(nc, "hub")
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	cfg := jetstream.StreamConfig{
		Name:     "NODES",
		Subjects: []string{"n.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	_, _ = js.Publish(ctx, "n.123.value", []byte("12"))
	_, _ = js.Publish(ctx, "n.123.value", []byte("13"))
	_, _ = js.Publish(ctx, "n.123.value", []byte("14"))
	_, _ = js.Publish(ctx, "n.123.value", []byte("15"))

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.123.>"))
	if err != nil {
		return fmt.Errorf("Error getting stream info: %w", err)
	}

	log.Println("Number of stream messages: ", si.State.Msgs)

	return nil
}

func leafConnectHubStream(srv *server.Server) error {
	url := srv.ClientURL()

	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.NewWithDomain(nc, "hub")
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.Stream(ctx, "NODES")

	if err != nil {
		return fmt.Errorf("Error opening stream: %w", err)
	}

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.123.>"))
	if err != nil {
		return fmt.Errorf("Error getting stream info: %w", err)
	}

	log.Println("Number of hub stream messages from leaf node: ", si.State.Msgs)

	return nil
}

func leafSourceHubStream(srv *server.Server) error {
	url := srv.ClientURL()

	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	cfg := jetstream.StreamConfig{
		Name:     "NODES",
		Subjects: []string{"n.>"},
		Sources: []*jetstream.StreamSource{
			{
				Name:   "NODES",
				Domain: "hub",
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	checkFor(5*time.Second, 100*time.Millisecond, func() error {
		si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.123.>"))
		if err != nil {
			return fmt.Errorf("Error getting stream info: %w", err)
		}

		if si.State.Msgs != 4 {
			return fmt.Errorf("Returned wrong number of messages: %v", si.State.Msgs)
		}

		log.Println("Number of leaf sourced stream messages: ", si.State.Msgs)
		return nil
	})

	return nil
}
