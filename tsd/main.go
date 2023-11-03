package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	log.Println("NATS TSD experiment")
	// Use the env variable if running in the container, otherwise use the default.
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	// Create an unauthenticated connection to NATS.
	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatal("Error connecting to nats: ", err)
	}

	// Drain is a safe way to to ensure all buffered messages that were published
	// are sent and all buffered messages received on a subscription are processed
	// being closing the connection.
	defer func() {
		err := nc.Drain()
		if err != nil {
			log.Println("Error draining: ", err)
		}
	}()

	// Access `JetStream` which provides methods to create
	// streams and consumers as well as convenience methods for publishing
	// to streams and consuming messages from the streams.
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("Error creating jetstream: ", err)
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
		log.Fatal("Error creating stream: ", err)
	}

	// clean up
	err = stream.Purge(ctx)
	if err != nil {
		log.Fatal("Error purging stream: ", err)
	}

	byteSlice := make([]byte, 4)

	ptCount := 10000

	start := time.Now()
	for i := uint32(0); i < uint32(ptCount); i++ {
		binary.LittleEndian.PutUint32(byteSlice, i)
		_, err := js.Publish(ctx, "n.123.value", byteSlice)
		if err != nil {
			log.Fatal("Error js.Publish: ", err)
		}
	}

	dur := time.Since(start).Seconds()
	log.Printf("js.Publish insert rate for 10,000 points: %v pts/sec\n", float64(ptCount)/dur)

	start = time.Now()
	for i := uint32(0); i < uint32(ptCount); i++ {
		binary.LittleEndian.PutUint32(byteSlice, i)
		_, err := js.PublishAsync("n.123.value", byteSlice)
		if err != nil {
			log.Fatal("Error nc.Publish: ", err)
		}
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(time.Second * 5):
		log.Fatal("publish async took too long")
	}

	dur = time.Since(start).Seconds()
	log.Printf("js.PublishAsync insert rate for 10,000 points: %v pts/sec\n", float64(ptCount)/dur)

	start = time.Now()
	for i := uint32(0); i < uint32(ptCount); i++ {
		binary.LittleEndian.PutUint32(byteSlice, i)
		err := nc.Publish("n.123.value", byteSlice)
		if err != nil {
			log.Fatal("Error nc.Publish: ", err)
		}
	}

	dur = time.Since(start).Seconds()
	log.Printf("nats.Publish insert rate for 10,000 points: %v pts/sec\n", float64(ptCount)/dur)
}
