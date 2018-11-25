package main

import (
	"crane/core/messages"
	"crane/core/utils"
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
func (c *Client) ContactDriver(topoMsg utils.TopologyMessage) {
	b, err := utils.Marshal(utils.TOPO_SUBMISSION, topoMsg)
	if err != nil {
		log.Println(err)
		return
	}
	c.Sub.Request <- messages.Message{
		Payload:      b,
		TargetConnId: c.Sub.Conn.RemoteAddr().String(),
		SourceConnId: c.Sub.Conn.LocalAddr().String(),
	}
}

func main() {
	client := NewClient(":5001")
	if client == nil {
		log.Println("Initialize client failed")
		return
	}
	tm := utils.TopologyMessage{}
	tm.Bolts = make([]utils.BoltMessage, 0)
	bm := utils.BoltMessage{
		Name:         "wordcount",
		InstNum:      4,
		PrevBoltName: "None",
		GroupingHint: utils.GROUPING_BY_SHUFFLE,
	}
	tm.Bolts = append(tm.Bolts, bm)
	client.ContactDriver(tm)
	client.Start()
}
