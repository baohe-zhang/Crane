package messages

// Message passing through the channels
type Message struct {
	TargetConnId string
	SourceConnId string
	Payload      []byte
}
