package pubsub

import (
	"bytes"
	rand "github.com/trpedersen/rand"
	"testing"
)

func randMsg() Msg {
	topic := rand.RandStr(100, "alphanum")
	bodyStr := rand.RandStr(1000, "alphanum")
	body := []byte(bodyStr)
	msg := NewMsg(topic, body)
	return msg
}

func TestMsgCreate(t *testing.T) {

	topic := rand.RandStr(100, "alphanum")
	bodyStr := rand.RandStr(1000, "alphanum")
	body := []byte(bodyStr)
	msg := NewMsg(topic, body)

	if msg.Topic() != topic {
		t.Errorf("expected %s, got %s", topic, msg.Topic())
	}

	if !bytes.Equal(body, msg.Body()) {
		t.Errorf("expected %s, got %s", body, msg.Body())
	}
}

func TestMsgClone(t *testing.T) {

	msg := randMsg()
	clone := msg.Clone()
	if msg.Topic() != clone.Topic() {
		t.Errorf("expected %s, got %s", msg.Topic(), clone.Topic())
	}

	if !bytes.Equal(msg.Body(), clone.Body()) {
		t.Errorf("expected %s, got %s", msg.Body(), clone.Body())
	}
}
