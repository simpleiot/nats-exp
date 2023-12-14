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
	srv, srvS, err := hubInit("")
	if err != nil {
		fmt.Println("Error init hub server: ", err)
		return
	}

	defer shutdown(srv, srvS)

	// start leaf instance
	srvLeaf, srvLeafS, err := leafInit("")
	if err != nil {
		fmt.Println("Error init leaf server: ", err)
		return
	}

	defer shutdown(srvLeaf, srvLeafS)

	// make sure leaf node is connected
	err = checkFor(5*time.Second, 100*time.Millisecond, func() error {
		nln := srv.NumLeafNodes()
		if nln != 1 {
			return fmt.Errorf("Expected 1 leaf node, got: %v", nln)
		}

		return nil
	})

	if err != nil {
		fmt.Println("Error waiting for leaf node to be connected: ", err)
		return
	}

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

	err = leafCreateStream(srv)
	if err != nil {
		fmt.Printf("Error creating leaf stream: %v\n", err)
		return
	}

	err = hubSourceLeafStream(srv)
	if err != nil {
		fmt.Printf("Error hub sourcing leaf stream: %v\n", err)
		return
	}

	// shutdown leaf node
	shutdown(srvLeaf, "")

	// make sure sourced stream is still available
	err = countStreamMessages(srv, "hub", "NODES-LEAF", 5, false)
	if err != nil {
		fmt.Printf("Error hub counting leaf stream messages after leaf server shut down")
		return
	}

	// publish some more messages to hub while leaf is powered down
	err = hubPublishMoreMessages(srv)
	if err != nil {
		fmt.Println("Error publishing more messages to hub: ", err)
		return
	}

	// start up leaf node
	srvLeaf, _, err = leafInit(srvLeafS)
	if err != nil {
		fmt.Println("Error init leaf server: ", err)
		return
	}

	// make sure hub messages get synced to leaf
	// FIXME for some reason we are only getting 4 messages here instead of 8
	err = countStreamMessages(srvLeaf, "leaf", "NODES-HUB", 8, false)
	if err != nil {
		fmt.Println("Error leaf counting hub sourced stream messages after leaf server powered back up: ", err)
	}

	err = leafPublishMoreMessages(srvLeaf)
	if err != nil {
		fmt.Println("Error publishing more messages to leaf: ", err)
	}

	err = countStreamMessages(srvLeaf, "leaf", "NODES-LEAF", 10, false)
	if err != nil {
		fmt.Println("Error leaf counting leaf stream messages after leaf server shut down: ", err)
	}

	// FIXME, for some reason we are getting 5 extra messages on the server that are not on the leaf node
	err = countStreamMessages(srv, "hub", "NODES-LEAF", 10, true)
	if err != nil {
		fmt.Println("Error hub counting leaf stream messages after leaf server shut down: ", err)
	}

	err = countStreamMessages(srvLeaf, "leaf", "NODES-LEAF", 10, true)
	if err != nil {
		fmt.Println("Error leaf counting leaf stream messages after leaf server shut down: ", err)
	}
}

func hubInit(storeDir string) (*server.Server, string, error) {
	var err error

	o := test.DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.JetStreamDomain = "hub"
	o.LeafNode.Host = o.Host
	o.NoSystemAccount = true
	o.LeafNode.Port = server.DEFAULT_LEAFNODE_PORT

	if storeDir != "" {
		o.StoreDir = storeDir
	} else if o.StoreDir, err = os.MkdirTemp("bi-directional-sync", "hub"); err != nil {
		return nil, "", fmt.Errorf("failed to create temporary jetstream directory: %v", err)
	}

	return test.RunServer(&o), o.StoreDir, nil
}

func leafInit(storeDir string) (*server.Server, string, error) {
	u, err := url.Parse("leafnode://127.0.0.1")
	if err != nil {
		return nil, "", fmt.Errorf("Error parsing URL: %w", err)
	}

	ol := test.DefaultTestOptions
	ol.Port = -1
	ol.JetStream = true
	ol.JetStreamDomain = "leaf"
	ol.LeafNode.Remotes = []*server.RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
		},
	}

	if storeDir != "" {
		ol.StoreDir = storeDir
	} else if ol.StoreDir, err = os.MkdirTemp("bi-directional-sync", "leaf"); err != nil {
		return nil, "", fmt.Errorf("failed to create temporary jetstream directory: %w", err)
	}

	return test.RunServer(&ol), ol.StoreDir, nil
}

func shutdown(srv *server.Server, storeDir string) {
	if srv != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
	}

	if storeDir != "" {
		_ = os.RemoveAll(storeDir)
	}
}

func checkFor(totalWait, sleepDur time.Duration, f func() error) error {
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		return fmt.Errorf(err.Error())
	}

	return nil
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
		Name:     "NODES-HUB",
		Subjects: []string{"n.hub.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("12"))
	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("13"))
	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("14"))
	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("15"))

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

	stream, err := js.Stream(ctx, "NODES-HUB")

	if err != nil {
		return fmt.Errorf("Error opening stream: %w", err)
	}

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.hub.123.>"))
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

	js, err := jetstream.NewWithDomain(nc, "leaf")
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	cfg := jetstream.StreamConfig{
		Name:     "NODES-HUB",
		Subjects: []string{"n.hub.>"},
		Sources: []*jetstream.StreamSource{
			{
				Name:   "NODES-HUB",
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

	return checkFor(5*time.Second, 100*time.Millisecond, func() error {
		si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.123.>"))
		if err != nil {
			return fmt.Errorf("Error getting stream info: %w", err)
		}

		if si.State.Msgs != 4 {
			return fmt.Errorf("Returned wrong number of messages: %v", si.State.Msgs)
		}

		log.Println("Number of hub->leaf sourced stream messages: ", si.State.Msgs)
		return nil
	})
}

func leafCreateStream(srv *server.Server) error {
	url := srv.ClientURL()

	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.NewWithDomain(nc, "leaf")
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	// The stream name and subjects must not conflict with what already exists

	cfg := jetstream.StreamConfig{
		Name:     "NODES-LEAF",
		Subjects: []string{"n.leaf.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on leaf: %w", err)
	}

	_, _ = js.Publish(ctx, "n.leaf.456.value", []byte("22"))
	_, _ = js.Publish(ctx, "n.leaf.456.value", []byte("23"))
	_, _ = js.Publish(ctx, "n.leaf.456.value", []byte("24"))
	_, _ = js.Publish(ctx, "n.leaf.456.value", []byte("25"))
	_, _ = js.Publish(ctx, "n.leaf.456.value", []byte("26"))

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.456.>"))
	if err != nil {
		return fmt.Errorf("Error getting stream info: %w", err)
	}

	log.Println("Number of leaf stream messages: ", si.State.Msgs)

	return nil
}

func hubSourceLeafStream(srv *server.Server) error {
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
		Name:     "NODES-LEAF",
		Subjects: []string{"n.leaf.>"},
		Sources: []*jetstream.StreamSource{
			{
				Name:   "NODES-LEAF",
				Domain: "leaf",
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	return checkFor(5*time.Second, 100*time.Millisecond, func() error {
		si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.leaf.456.>"))
		if err != nil {
			return fmt.Errorf("Error getting stream info: %w", err)
		}

		if si.State.Msgs != 5 {
			return fmt.Errorf("Returned wrong number of messages: %v", si.State.Msgs)
		}

		log.Println("Number of leaf->hub sourced stream messages: ", si.State.Msgs)
		return nil
	})
}

func countStreamMessages(srv *server.Server, domain, strName string, expected int, dump bool) error {
	url := srv.ClientURL()

	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.NewWithDomain(nc, domain)
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.Stream(ctx, strName)
	if err != nil {
		return fmt.Errorf("Error opening stream on hub: %w", err)
	}

	defer func() {
		if dump {
			si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.leaf.456.>"))
			if err != nil {
				fmt.Printf("Error getting stream info: %v", err)
				return
			}

			for i := si.State.FirstSeq; i <= si.State.LastSeq; i++ {
				msg, err := stream.GetMsg(ctx, i)
				if err != nil {
					fmt.Printf("Error getting message: %v", err)
					return
				}

				fmt.Printf("Message %v: %v\n", msg.Sequence, string(msg.Data))
			}
		}
	}()

	return checkFor(5*time.Second, 100*time.Millisecond, func() error {
		si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.leaf.456.>"))
		if err != nil {
			return fmt.Errorf("Error getting stream info: %w", err)
		}

		if si.State.Msgs != uint64(expected) {
			return fmt.Errorf("Returned wrong number of messages, expected: %v, got: %v", expected, si.State.Msgs)
		}

		log.Printf("Number of stream messages:%v:%v:%v\n", domain, strName, si.State.Msgs)
		return nil
	})
}

func leafPublishMoreMessages(srv *server.Server) error {
	url := srv.ClientURL()

	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.NewWithDomain(nc, "leaf")
	if err != nil {
		return fmt.Errorf("Error creating Jetstream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.Stream(ctx, "NODES-LEAF")
	if err != nil {
		return fmt.Errorf("Error opening stream on leaf: %w", err)
	}

	_, _ = js.PublishAsync("n.leaf.456.value", []byte("27"))
	_, _ = js.PublishAsync("n.leaf.456.value", []byte("28"))
	_, _ = js.PublishAsync("n.leaf.456.value", []byte("29"))
	_, _ = js.PublishAsync("n.leaf.456.value", []byte("30"))
	_, _ = js.PublishAsync("n.leaf.456.value", []byte("31"))

	select {
	case <-js.PublishAsyncComplete():
		break
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Timeout waiting for publish to complete")
	}

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.456.>"))
	if err != nil {
		return fmt.Errorf("Error getting stream info: %w", err)
	}

	log.Println("Number of leaf stream messages: ", si.State.Msgs)

	return nil
}

func hubPublishMoreMessages(srv *server.Server) error {
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

	stream, err := js.Stream(ctx, "NODES-HUB")
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("16"))
	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("17"))
	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("18"))
	_, _ = js.Publish(ctx, "n.hub.123.value", []byte("19"))

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.123.>"))
	if err != nil {
		return fmt.Errorf("Error getting stream info: %w", err)
	}

	log.Println("Number of hub stream messages: ", si.State.Msgs)

	return nil
}
