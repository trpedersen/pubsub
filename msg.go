package pubsub

type Msg interface {
	Topic() string
	Body() []byte
	Clone() Msg
}

type msg struct {
	topic string
	body  []byte
}

// NewMsg creates a new Msg. It uses value semantics for Msg attributes.
// Msg.Body() will contain a *copy* of the passed in body.
func NewMsg(topic string, body []byte) Msg {
	msg := &msg{
		topic: topic,
		body:  make([]byte, len(body), cap(body)),
	}
	copy(msg.body, body)
	return msg
}

func (msg *msg) Topic() string {
	return msg.topic
}

func (msg *msg) Body() []byte {
	return msg.body
}

// Clone creates a new Msg from src. It uses value semantics.
func (msg *msg) Clone() Msg {
	clone := NewMsg(msg.Topic(), msg.Body())
	return clone
}
