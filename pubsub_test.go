package pubsub

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"testing"

	rand "github.com/trpedersen/rand"
)

const (
	TOPICS                = 100
	MSG_PER_TOPIC         = 100
	SUBSCRIBERS_PER_TOPIC = 10 //00
)

func merge(done <-chan struct{}, cs ...<-chan Msg) <-chan Msg {

	var wg sync.WaitGroup
	out := make(chan Msg)

	output := func(c <-chan Msg) {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-c:
				if ok {
					out <- msg
				} else {
					return
				}
			case <-done:
				return
			}
		}

	}

	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func sink(done chan struct{}, in <-chan Msg) <-chan int {

	maxCount := TOPICS * SUBSCRIBERS_PER_TOPIC * MSG_PER_TOPIC
	log.Printf("sink: maxCount: %d\n", maxCount)
	count := 0

	var wg sync.WaitGroup
	out := make(chan int)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for count < maxCount {
			select {
			case _, ok := <-in:
				if ok {
					count += 1
					if count%1000 == 0 {
						log.Println(count)
					}
				} else {
					return
				}
			case <-done:
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		out <- count
		close(out)
	}()

	return out
}

func consume(done <-chan struct{}, subscription Subscription) <-chan Msg {

	out := make(chan Msg)

	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-subscription.Delivery():
				if ok {
					out <- msg
				} else {
					return
				}
			case <-done:
				return
			}
		}
	}()

	return out
}

func TestPubSub(t *testing.T) {

	nCPU := runtime.NumCPU()
	log.Println("nCPU: ", nCPU)

	//runtime.GOMAXPROCS(nCPU)

	hub, _ := NewHub()

	done := make(chan struct{})

	log.Print("making topics...")
	topics := make([]string, 0)
	for i := 0; i < TOPICS; i++ {
		topics = append(topics, fmt.Sprintf("TOPIC.%d", i))
	}
	log.Println("...making topics done")

	log.Print("making subscriptions...")
	deliveries := make([]<-chan Msg, 0)
	for _, topic := range topics {
		for i := 0; i < SUBSCRIBERS_PER_TOPIC; i++ {
			sub, err := hub.Subscribe(topic)
			if err != nil {
				t.Errorf("hub.Subscribe returned err: %s\n", err)
			} else {
				deliveries = append(deliveries, sub.Delivery())
			}
		}
	}
	log.Println("...making subscriptions done")

	sinkC := sink(done, merge(done, deliveries...))

	log.Print("publishing topics...")
	for _, topic := range topics {
		for i := 0; i < MSG_PER_TOPIC; i++ {
			bodyStr := rand.RandStr(1000, "alphanum")
			body := []byte(bodyStr)
			msg := NewMsg(topic, body)
			hub.Publish(msg)
		}
	}
	log.Print("...publishing topics done")

	count := <-sinkC

	log.Print("halting...")
	close(done)
	hub.Halt()
	log.Print("...halted")

	log.Printf("stats: %s, messages back: %d\n", hub.Stats(), count)

}
