package pubsub

type Subscription interface {
	Topic() string
	Delivery() <-chan Msg
	Close() error
}

type sub struct {
	topic    string
	delivery chan Msg
	closed   chan struct{}
}

func (s *sub) Topic() string {
	return s.topic
}

func (s *sub) Delivery() <-chan Msg {
	return s.delivery
}

// Close a subscription. Idempotent.
func (s *sub) Close() error {
	select {
	case <-s.closed:
		// already closed
		return nil
	default:
		close(s.closed)
		close(s.delivery)
		return nil
	}
}
