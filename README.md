# pubsub
Publish/Subscribe in Go

Implement publish/subscribe using channels, goroutines. 

Surprisingly high-performant, processing 100s of thousands of messages/sec with message copy semantics, using standard go channels, maps & goroutines. Can do more than a million messages/sec if you don't use copy semantics for sent messages.


