package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/test"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {

	var err error

	// create an in-memory test server
	opts := test.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true

	if opts.StoreDir, err = os.MkdirTemp("tsd", "test"); err != nil {
		log.Fatalf("failed to create temporary jetstream directory: %v", err)
	}

	srv := test.RunServer(&opts)

	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		// remove jetstream state
		_ = os.RemoveAll(opts.StoreDir)
	}()

	log.Println("NATS TSD experiment")

	// Create an unauthenticated connection to NATS.
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		log.Fatal("Error connecting to nats: ", err)
	}

	// Drain is a safe way to ensure all buffered messages that were published
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

	jsSyncPublish(ctx, js, 10000)
	jsAsyncPublish(ctx, js, 10000)
	natsPublish(ctx, nc, 10000)
	readAllMessages(stream, false)
	readAllMessages(stream, true)
	readAllMessages(stream, false)
}

func jsSyncPublish(ctx context.Context, js jetstream.JetStream, count int) {
	// test sync jetstream publish
	start := time.Now()
	byteSlice := make([]byte, 4)
	for i := uint32(0); i < uint32(count); i++ {
		binary.LittleEndian.PutUint32(byteSlice, i)
		_, err := js.Publish(ctx, "n.123.value", byteSlice)
		if err != nil {
			log.Fatal("Error js.Publish: ", err)
		}
	}

	dur := time.Since(start).Seconds()
	log.Printf("js.Publish insert rate for 10,000 points: %.0f pts/sec\n", float64(count)/dur)
}

func jsAsyncPublish(ctx context.Context, js jetstream.JetStream, count int) {
	// test async jetstream publish
	start := time.Now()
	byteSlice := make([]byte, 4)
	for i := uint32(0); i < uint32(count); i++ {
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

	dur := time.Since(start).Seconds()
	log.Printf("js.PublishAsync insert rate for 10,000 points: %.0f pts/sec\n", float64(count)/dur)
}

func natsPublish(ctx context.Context, nc *nats.Conn, count int) {
	// test nats publish
	start := time.Now()
	byteSlice := make([]byte, 4)
	for i := uint32(0); i < uint32(count); i++ {
		binary.LittleEndian.PutUint32(byteSlice, i)
		err := nc.Publish("n.123.value", byteSlice)
		if err != nil {
			log.Fatal("Error nc.Publish: ", err)
		}
	}

	dur := time.Since(start).Seconds()
	log.Printf("nats.Publish insert rate for 10,000 points: %.0f pts/sec\n", float64(count)/dur)

}

func readAllMessages(stream jetstream.Stream, ack bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// test time to get data all data from stream
	start := time.Now()
	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatal("Error getting stream info: ", err)
	}

	// subscribe to the subject and read back all the messages

	consumerCfg := jetstream.ConsumerConfig{
		// ack messages in batches
		AckPolicy:     jetstream.AckAllPolicy,
		MaxAckPending: 1024 * 10,
	}

	if !ack {
		consumerCfg.AckPolicy = jetstream.AckNonePolicy
	}

	consumer, err := stream.CreateConsumer(ctx, consumerCfg)

	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	msgCtx, err := consumer.Messages()
	if err != nil {
		log.Fatalf("failed to create a msg ctx: %v", err)
	}

	var msg jetstream.Msg
	var meta *jetstream.MsgMetadata

	count := 0

	for {
		msg, err = msgCtx.Next()
		if err != nil {
			log.Fatalf("error receiving next msg: %v", err)
		}

		meta, err = msg.Metadata()
		if err != nil {
			log.Fatalf("failed to retrieve msg metadata: %v", err)
		}

		if ack {
			// ack the msg whenever we reach a batch boundary
			if meta.Sequence.Stream%uint64(consumerCfg.MaxAckPending) == 0 {
				if err = msg.Ack(); err != nil {
					log.Fatalf("failed to ack msg: %v", err)
				}
			}
		}

		count++

		if meta.NumPending == 0 {
			// no more messages in the stream
			break
		}
	}

	// ack last batch
	if err = msg.Ack(); err != nil {
		log.Fatalf("failed to ack msg: %v", err)
	}

	dur := time.Since(start).Seconds()
	cnt := meta.Sequence.Stream - info.State.FirstSeq + 1
	log.Printf("ack: %v, Get %v points took %.2f, %.0f pts/sec\n", ack, count, dur,
		float64(cnt)/dur)
}
