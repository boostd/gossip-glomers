package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"os"
	"sync"
	"time"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, seen: make(map[int]bool), missedUpdates: make(map[string]map[int]bool)}

	n.Handle("broadcast", s.HandleBroadcast)
	n.Handle("read", s.HandleRead)
	n.Handle("topology", s.HandleTopology)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.GossipMissedUpdates()
			}
		}
	}()

	if err := n.Run(); err != nil {
		errorString := fmt.Sprintf("ERROR, %s", err)
		os.Stderr.WriteString(errorString)
		os.Exit(1)
	}
}

type Topology map[string][]string
type server struct {
	n        *maelstrom.Node
	topology Topology

	messages []int
	seen     map[int]bool

	mu            sync.RWMutex
	missedUpdates map[string]map[int]bool
}
type Broadcast struct {
	Message int `json:"message"`
}

func (s *server) HandleBroadcast(msg maelstrom.Message) error {
	var msgBody Broadcast
	if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
		return err
	}
	replyBody := make(map[string]any)
	replyBody["type"] = "broadcast_ok"
	message := msgBody.Message

	s.mu.Lock()
	seen := s.seen[message] == true
	s.seen[message] = true
	s.mu.Unlock()

	if seen {
		return s.n.Reply(msg, replyBody)
	}

	s.messages = append(s.messages, message)
	for _, dest := range s.topology[s.n.ID()] {
		if dest == msg.Src {
			continue
		}
		go s.Gossip(dest, message)
	}

	return s.n.Reply(msg, replyBody)
}

func (s *server) Gossip(destination string, message int) {
	s.mu.Lock()
	if s.missedUpdates[destination] == nil {
		s.missedUpdates[destination] = make(map[int]bool)
	}
	s.missedUpdates[destination][message] = true
	s.mu.Unlock()

	broadcastBody := map[string]any{"type": "broadcast", "message": message}
	if err := s.n.RPC(destination, broadcastBody, s.GossipResponse(message)); err != nil {
		errorString := fmt.Sprintf("ERROR, %s", err)
		os.Stderr.WriteString(errorString)
	}
}

func (s *server) GossipResponse(value int) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		if msg.Type() != "broadcast_ok" {
			return nil
		}

		s.HandleBroadcastOk(msg.Src, value)
		return nil
	}
}

func (s *server) HandleBroadcastOk(node string, message int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	messagesMissed := s.missedUpdates[node]
	if len(messagesMissed) == 0 {
		return
	}

	delete(messagesMissed, message)
	s.missedUpdates[node] = messagesMissed
}

func (s *server) GossipMissedUpdates() {
	for node, updates := range s.missedUpdates {
		for update := range updates {
			s.Gossip(node, update)
		}
	}
}

func (s *server) HandleTopology(msg maelstrom.Message) error {
	var msgBody struct {
		Topology Topology `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
		return err
	}

	s.topology = msgBody.Topology

	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) HandleRead(msg maelstrom.Message) error {
	replyBody := make(map[string]any)
	replyBody["type"] = "read_ok"
	replyBody["messages"] = s.messages

	return s.n.Reply(msg, replyBody)
}
