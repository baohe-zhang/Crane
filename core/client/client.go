package client

import (
	"crane/core/messages"
	"log"
)

// Client, the instance for client to submit
// tasks and contact with the master node
type Client struct {
	Sub *messages.Subscriber
}

// Factory mode to return the Client instance
func NewClient(driverAddr string) *Client {
	client := &Client{}
	client.Sub = messages.NewSubscriber(driverAddr)
	if client.Sub == nil {
		return nil
	}
	return client
}

// Client instance start to submit topology message
// after reveicing acknowledgment, it would terminate
func (c *Client) Start() {
	go c.Sub.RequestMessage()
	go c.Sub.ReadMessage()
	for {
		select {
		case rcvMsg := <-c.Sub.PublishBoard:
			log.Printf("Receive Message from %s: %s", rcvMsg.SourceConnId, rcvMsg.Payload)
			return
		default:

		}
	}
}

// Contact driver node to notify the topology should be computed and scheduled
func (c *Client) ContactDriver(msg []byte) {
	c.Sub.Request <- messages.Message{
		Payload:      msg,
		TargetConnId: c.Sub.Conn.RemoteAddr().String(),
		SourceConnId: c.Sub.Conn.LocalAddr().String(),
	}
}
