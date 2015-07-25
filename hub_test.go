package pubsub

import (
	"bytes"
	"sync"
	"testing"
)

func TestHubRunHalt(t *testing.T) {
	hub, err := NewHub()
	if err != nil {
		t.Errorf("NewHub() returned error: %s", err)
	}

	if !hub.IsRunning() {
		t.Error("hub is not running")
	}

	err = hub.Halt() // should block
	if err != nil {
		t.Errorf("hub.Halt() returned error: %s", err)
	} else {
		if hub.IsRunning() {
			t.Errorf("hub did not halt")
		}
	}
	// test idempotent
	err = hub.Halt() // should block
	if err != nil {
		t.Errorf("hub.Halt() returned error: %s", err)
	}
}

func TestPubSubSingle(t *testing.T) {
	hub, err := NewHub()
	if err != nil {
		t.Errorf("NewHub() returned error: %s", err)
	}

	msg := randMsg()
	var msgBack Msg
	var wg sync.WaitGroup

	sub, err := hub.Subscribe(msg.Topic())
	if err != nil {
		t.Errorf("hub.Subcribe returned err: %s", err)
	}

	wg.Add(1)
	go func() {
		msgBack = <-sub.Delivery()
		wg.Done()
	}()

	hub.Publish(msg)

	wg.Wait()
	//log.Println(msg)
	//log.Println(msgBack)

	if msgBack.Topic() != msg.Topic() {
		t.Errorf("wrong topic, expected %s, got %s\n", msg.Topic(), msgBack.Topic())
	}

	if !bytes.Equal(msg.Body(), msgBack.Body()) {
		t.Errorf("wrong bytes[], expected %s, got %s", msg.Body(), msgBack.Body())
	}

	//log.Printf("%p  %p", &msg.Body()[0], &msgBack.Body()[0])

	sub.Close()
	hub.Halt()

}

func TestPubSub1000(t *testing.T) {
	hub, err := NewHub()
	if err != nil {
		t.Errorf("NewHub() returned error: %s", err)
	}

	for i := 0; i < 1000; i++ {
		msg := randMsg()
		var msgBack Msg
		var wg sync.WaitGroup

		sub, err := hub.Subscribe(msg.Topic())
		if err != nil {
			t.Errorf("hub.Subcribe returned err: %s", err)
		}

		wg.Add(1)
		go func() {
			msgBack = <-sub.Delivery()
			wg.Done()
		}()

		hub.Publish(msg)

		wg.Wait()
		//log.Println(msg)
		//log.Println(msgBack)

		if msgBack.Topic() != msg.Topic() {
			t.Errorf("wrong topic, expected %s, got %s\n", msg.Topic(), msgBack.Topic())
		}

		if !bytes.Equal(msg.Body(), msgBack.Body()) {
			t.Errorf("wrong bytes[], expected %s, got %s", msg.Body(), msgBack.Body())
		}

		//log.Printf("%p  %p", &msg.Body()[0], &msgBack.Body()[0])

		sub.Close()
	}
	hub.Halt()

}
