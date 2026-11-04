package server

import "sync"

const SubscriptionBufferSize = 100

type Broker struct {
	mu      sync.RWMutex
	topics  map[string]map[int]*Subscriber
	nextID  int
	nextSeq map[string]uint64
}

type Message struct {
	Seq  uint64
	Data []byte
}

type Subscriber struct {
	messages chan Message
}

func NewBroker() *Broker {
	return &Broker{
		topics:  make(map[string]map[int]*Subscriber),
		nextSeq: make(map[string]uint64),
	}
}

func (b *Broker) Subscribe(topic string) (int, *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; !ok {
		b.topics[topic] = make(map[int]*Subscriber)
	}

	id := b.nextID
	b.nextID++

	sub := &Subscriber{messages: make(chan Message, SubscriptionBufferSize)}
	b.topics[topic][id] = sub

	return id, sub
}

func (b *Broker) Unsubscribe(topic string, id int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if subs, ok := b.topics[topic]; ok {
		if sub, ok := subs[id]; ok {
			close(sub.messages)
			delete(subs, id)
			if len(subs) == 0 {
				delete(b.topics, topic)
			}
			return true
		}
	}
	return false
}

func (b *Broker) Publish(topic string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	seq := b.nextSeq[topic]
	b.nextSeq[topic]++

	msg := Message{Seq: seq, Data: data}

	if subs, ok := b.topics[topic]; ok {
		for _, sub := range subs {
			select {
			case sub.messages <- msg:
			default:
				// Drop message, subscriber's buffer is full
			}
		}
	}
}

func (b *Broker) Event(topic string, id int) <-chan Message {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if subs, ok := b.topics[topic]; ok {
		if sub, ok := subs[id]; ok {
			return sub.messages
		}
	}
	return nil
}
