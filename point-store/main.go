package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Start embedded NATS server with JetStream enabled
	o := test.DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	var err error
	if o.StoreDir, err = os.MkdirTemp("", "point-store"); err != nil {
		log.Fatalf("Failed to create temporary jetstream directory: %v", err)
	}
	
	srv := test.RunServer(&o)
	defer func() {
		srv.Shutdown()
		os.RemoveAll(o.StoreDir)
	}()

	// Create an unauthenticated connection to the embedded NATS server.
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}

	// Drain is a safe way to to ensure all buffered messages that were published
	// are sent and all buffered messages received on a subscription are processed
	// being closing the connection.
	defer nc.Drain()

	// Access `JetStream` which provides methods to create
	// streams and consumers as well as convenience methods for publishing
	// to streams and consuming messages from the streams.
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	// We will declare the initial stream configuration by specifying
	// the name and subjects. Stream names are commonly uppercased to
	// visually differentiate them from subjects, but this is not required.
	// A stream can bind one or more subjects which almost always include
	// wildcards. In addition, no two streams can have overlapping subjects
	// otherwise the primary messages would be persisted twice. There
	// are option to replicate messages in various ways, but that will
	// be explained in later examples.
	cfg := jetstream.StreamConfig{
		Name:     "NODES",
		Subjects: []string{"n.>"},
	}

	// JetStream API uses context for timeouts and cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Finally, let's add/create the stream with the default (no) limits.
	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}

	// Let's publish messages for several different "nodes".
	js.Publish(ctx, "n.123.description", []byte("Injector temperature"))
	js.Publish(ctx, "n.123.type", []byte("temp"))
	js.Publish(ctx, "n.123.value", []byte("12"))
	js.Publish(ctx, "n.123.value", []byte("13"))
	js.Publish(ctx, "n.123.value", []byte("14"))
	js.Publish(ctx, "n.123.value", []byte("15"))
	js.Publish(ctx, "n.123.location", []byte("building A"))

	js.Publish(ctx, "n.456.description", []byte("Exhaust temperature"))
	js.Publish(ctx, "n.456.value", []byte("563"))

	js.Publish(ctx, "n.789.description", []byte("Internal temperature"))
	js.Publish(ctx, "n.789.value", []byte("423"))

	// get state of n.123 and ignore others
	si, err := stream.Info(ctx, jetstream.WithSubjectFilter("n.123.>"))
	if err != nil {
		log.Fatalf("Failed to get stream info: %v", err)
	}

	// create a map to hold the state of n.123
	state := make(map[string]string)

	// loop through all of the n.i23.> subjects and get the last
	// message
	for k := range si.State.Subjects {
		m, err := stream.GetLastMsgForSubject(ctx, k)
		if err != nil {
			log.Fatal("Error getting last message: ", err)
		}
		state[k] = string(m.Data)
	}

	fmt.Printf("Current stream state: %+v\n", state)
}
