package pubsub

import (
	"fmt"
	"sync/atomic"
)

type Hub interface {
	IsRunning() bool
	Halt() error

	Publish(msg Msg) error
	Subscribe(topic string) (Subscription, error)

	Stats() string
}

type subscribeCmd struct {
	topic string
	reply chan subscribeEvent
}

type subscribeEvent struct {
	subscription *sub
	err          error
}

type publishCmd struct {
	msg  Msg
	errc chan error
}

type hub struct {
	// halt   chan chan error
	halted chan struct{}
	ping   chan chan struct{}

	subscribe chan *subscribeCmd
	publish   chan *publishCmd

	subscriptions map[string]map[*sub]bool

	msg_sent  int64
	msg_pub   int64
	sub_count int64
}

// NewHub creates a new hub and starts it running in a goroutine.
func NewHub() (Hub, error) {
	hub := &hub{
		halted:    make(chan struct{}),
		subscribe: make(chan *subscribeCmd),
		publish:   make(chan *publishCmd),

		subscriptions: make(map[string]map[*sub]bool),
	}
	go hub.run()
	return hub, nil
}

func (hub *hub) run() {
run:
	for {
		select {
		case <-hub.halted:
			break run
		case cmd := <-hub.subscribe:
			hub.addSubscription(cmd)
		case cmd := <-hub.publish:
			hub.publishMsg(cmd)
		}
	}
	close(hub.halted)
	return
}

func (hub *hub) addSubscription(cmd *subscribeCmd) {
	s := &sub{
		topic:    cmd.topic,
		delivery: make(chan Msg, 1000),
		closed:   make(chan struct{}),
	}
	subs, exists := hub.subscriptions[cmd.topic]
	if !exists {
		subs = make(map[*sub]bool)
		hub.subscriptions[cmd.topic] = subs
	}
	subs[s] = true
	atomic.AddInt64(&hub.sub_count, 1)
	go func() {
		cmd.reply <- subscribeEvent{subscription: s, err: nil}
	}()
	return
}

func (hub *hub) dropSubscription(sub *sub) {
	if subs, exists := hub.subscriptions[sub.topic]; exists {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(hub.subscriptions, sub.topic)
		}
	}
	// TODO: sub should have got here after Close()...revisit?
}

func (hub *hub) publishMsg(cmd *publishCmd) error {
	msg := cmd.msg
	if subs, exists := hub.subscriptions[msg.Topic()]; exists {
		for sub, _ := range subs {
			select {
			case <-sub.closed:
				hub.dropSubscription(sub)
			default:
				sub.delivery <- msg.Clone()
				atomic.AddInt64(&hub.msg_sent, 1)
				// go func() {
				// 	sub.delivery <- msg.Clone()
				// 	atomic.AddInt64(&hub.msg_sent, 1)
				// }()
			}
		}
	}
	atomic.AddInt64(&hub.msg_pub, 1)
	cmd.errc <- nil
	return nil
}

func (hub *hub) IsRunning() bool {
	select {
	case <-hub.halted:
		return false
	default:
		return true
	}
}

// Halt the hub. Idempotent.
func (hub *hub) Halt() error {
	select {
	case <-hub.halted: // already closed
		return nil
	default:
		hub.halted <- struct{}{}
	}
	<-hub.halted
	return nil
}

func (hub *hub) Subscribe(topic string) (Subscription, error) {
	cmd := &subscribeCmd{
		topic: topic,
		reply: make(chan subscribeEvent),
	}
	hub.subscribe <- cmd
	event := <-cmd.reply
	return event.subscription, event.err
}

func (hub *hub) Publish(msg Msg) error {
	cmd := &publishCmd{
		msg:  msg,
		errc: make(chan error),
	}
	hub.publish <- cmd
	return <-cmd.errc
}

func (hub *hub) Stats() string {
	return fmt.Sprintf("sub_count: %d, msg_pub: %d, msg_sent: %d", hub.sub_count, hub.msg_pub, hub.msg_sent)
}
