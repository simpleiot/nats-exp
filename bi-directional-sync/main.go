package main

import (
	"context"
	"fmt"
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

	heading("Create stream on hub and source to leaf")

	err = createStream(srv, "hub", "NODES-HUB", "n.hub.>")
	if err != nil {
		fmt.Printf("Error creating hub stream: %v\n", err)
		return
	}

	err = publishMessages(srv, "n.hub.123.value", []string{"12", "13", "14", "15"})
	if err != nil {
		fmt.Println("Error publishing more messages to hub: ", err)
		return
	}

	_, err = getMessages(srv, "hub", "NODES-HUB", 4)
	if err != nil {
		fmt.Printf("Error hub counting hub stream messages: %v", err)
	}

	// count messages in hub stream from leaf node. This works because
	// the leaf node can see everything on the hub and forwards requests.
	_, err = getMessages(srvLeaf, "hub", "NODES-HUB", 4)
	if err != nil {
		fmt.Printf("Error hub counting hub stream messages: %v", err)
	}

	// before we source stream, we should not see any messages in the leaf domain
	_, err = getMessages(srvLeaf, "leaf", "NODES-HUB", 0)
	if err == nil {
		fmt.Printf("expected error sourcing messages before sourced: %v", err)
	}

	err = sourceStream(srvLeaf, "leaf", "NODES-HUB", "hub", "n.hub.>")
	if err != nil {
		fmt.Printf("Error leaf sourcing hub stream: %v\n", err)
		return
	}

	// count messages in sourced stream
	_, err = getMessages(srvLeaf, "leaf", "NODES-HUB", 4)
	if err != nil {
		fmt.Printf("Error leaf counting messages sourced from hub: %v", err)
	}

	heading("Create stream on leaf and source to hub")

	// now, create a stream on leaf node and source to hub
	err = createStream(srvLeaf, "leaf", "NODES-LEAF", "n.leaf.>")
	if err != nil {
		fmt.Printf("Error creating leaf stream: %v\n", err)
		return
	}

	err = publishMessages(srvLeaf, "n.leaf.456.value", []string{"22", "23", "24", "25", "26"})
	if err != nil {
		fmt.Println("Error publishing more messages to hub: ", err)
		return
	}

	err = sourceStream(srv, "hub", "NODES-LEAF", "leaf", "n.leaf.>")
	if err != nil {
		fmt.Printf("Error hub sourcing leaf stream: %v\n", err)
		return
	}

	_, err = getMessages(srv, "hub", "NODES-LEAF", 5)
	if err != nil {
		fmt.Printf("Error hub counting messages sourced from leaf: %v", err)
	}

	// heading("Shut down leaf node")
	// shutdown leaf node
	//shutdown(srvLeaf, "")

	// make sure sourced stream is still available
	_, err = getMessages(srv, "hub", "NODES-LEAF", 5)
	if err != nil {
		fmt.Printf("Error hub counting leaf stream messages after leaf server shut down")
		return
	}

	// publish some more messages to hub while leaf is powered down
	err = publishMessages(srv, "n.hub.123.value", []string{"16", "17", "18", "19"})
	if err != nil {
		fmt.Println("Error publishing more messages to hub: ", err)
		return
	}

	_, err = getMessages(srv, "hub", "NODES-HUB", 8)
	if err != nil {
		fmt.Printf("Error hub counting hub stream messages while leaf is down: %v", err)
	}

	// heading("Start up leaf node")
	// start up leaf node
	/*
		srvLeaf, _, err = leafInit(srvLeafS)
		if err != nil {
			fmt.Println("Error init leaf server: ", err)
			return
		}
	*/

	// make sure hub messages get synced to leaf
	// FIXME for some reason we are only getting 4 messages here instead of 8
	_, err = getMessages(srvLeaf, "leaf", "NODES-HUB", 8)
	if err != nil {
		fmt.Println("Error leaf counting hub sourced stream messages after leaf server powered back up: ", err)
	}

	err = publishMessages(srv, "n.hub.123.value", []string{"20", "21", "22", "23"})
	if err != nil {
		fmt.Println("Error publishing more messages to hub: ", err)
		return
	}

	msgs, err := getMessages(srvLeaf, "leaf", "NODES-HUB", 12)
	if err != nil {
		fmt.Println("Error leaf counting hub sourced stream messages after leaf server powered back up: ", err)
		msgs.dump()
	}

	heading("Publish more messages to leaf")

	err = publishMessages(srvLeaf, "n.leaf.456.value", []string{"27", "28", "29", "30", "31"})
	if err != nil {
		fmt.Println("Error publishing more messages to leaf: ", err)
	}

	_, err = getMessages(srvLeaf, "leaf", "NODES-LEAF", 10)
	if err != nil {
		fmt.Println("Error leaf counting leaf stream messages after leaf server shut down: ", err)
	}

	// FIXME, for some reason we are getting 5 extra messages on the server that are not on the leaf node
	msgs, err = getMessages(srv, "hub", "NODES-LEAF", 10)
	if err != nil {
		fmt.Println("Error hub counting leaf stream messages after leaf server shut down: ", err)
		msgs.dump()
	}

	msgs, err = getMessages(srvLeaf, "leaf", "NODES-LEAF", 10)
	if err != nil {
		fmt.Println("Error leaf counting leaf stream messages after leaf server shut down: ", err)
	}

	msgs.dump()
}

func heading(s string) {
	fmt.Println("=====================================================")
	fmt.Println("       " + s)
	fmt.Println("=====================================================")
}

func hubInit(storeDir string) (*server.Server, string, error) {
	var err error

	o := test.DefaultTestOptions
	o.ServerName = "hub"
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
	ol.ServerName = "leaf"
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

func createStream(srv *server.Server, domain, stream, subject string) error {
	fmt.Printf("* Create stream: server:%v domain:%v stream:%v subject:%v\n", srv.Name(), domain, stream, subject)
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

	cfg := jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	return nil
}

func sourceStream(srv *server.Server, domain, stream, sourceDomain, subject string) error {
	fmt.Printf("* Source stream: server:%v domain:%v stream:%v source-domain:%v subject:%v\n",
		srv.Name(), domain, stream, sourceDomain, subject)
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

	cfg := jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Sources: []*jetstream.StreamSource{
			{
				Name:   stream,
				Domain: sourceDomain,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("Error creating stream on hub: %w", err)
	}

	return nil
}

type message struct {
	seq int
	msg string
}

type messages struct {
	server string
	domain string
	stream string
	msgs   []message
}

func (m *messages) dump() {
	fmt.Printf("* Dump messages: server:%v domain:%v stream:%v\n", m.server, m.domain, m.stream)
	for _, ms := range m.msgs {
		fmt.Printf("  Message %v: %v\n", ms.seq, ms.msg)
	}
}

func getMessages(srv *server.Server, domain, strName string, expected int) (messages, error) {
	url := srv.ClientURL()

	ret := messages{
		server: srv.Name(),
		domain: domain,
		stream: strName,
	}

	nc, err := nats.Connect(url)
	if err != nil {
		return ret, fmt.Errorf("Error connecting: %w", err)
	}

	defer func() {
		_ = nc.Drain()
	}()

	js, err := jetstream.NewWithDomain(nc, domain)
	if err != nil {
		return ret, fmt.Errorf("Error creating Jetstream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.Stream(ctx, strName)
	if err != nil {
		return ret, fmt.Errorf("Error opening stream on hub: %w", err)
	}

	var cntErr error

	if expected >= 0 {
		cntErr = checkFor(5*time.Second, 100*time.Millisecond, func() error {
			si, err := stream.Info(ctx)
			if err != nil {
				return fmt.Errorf("Error getting stream info: %w", err)
			}

			if si.State.Msgs != uint64(expected) {
				return fmt.Errorf("Returned wrong number of messages, expected: %v, got: %v", expected, si.State.Msgs)
			}

			fmt.Printf("* Stream count: server:%v domain:%v stream:%v count:%v\n", srv.Name(), domain, strName, si.State.Msgs)
			return nil
		})
	}

	si, err := stream.Info(ctx)
	if err != nil {
		return ret, fmt.Errorf("Error getting stream info: %w", err)
	}

	for i := si.State.FirstSeq; i <= si.State.LastSeq; i++ {
		msg, err := stream.GetMsg(ctx, i)
		if err != nil {
			return ret, fmt.Errorf("Error getting message: %w", err)
		}

		ret.msgs = append(ret.msgs, message{seq: int(msg.Sequence), msg: string(msg.Data)})
	}

	return ret, cntErr

}

func msgs2String(msgs []string) string {
	ret := ""
	for _, m := range msgs {
		ret += m + " "
	}

	return ret
}

func publishMessages(srv *server.Server, subject string, messages []string) error {
	fmt.Printf("* Publish: %v -> %v -> %v\n", srv.Name(), subject, msgs2String(messages))
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

	for _, m := range messages {
		_, err = js.Publish(ctx, subject, []byte(m))
		if err != nil {
			return fmt.Errorf("Publish error: %w", err)
		}
	}

	return nil
}
