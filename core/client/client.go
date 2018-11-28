package main

import (
	"crane/bolt"
	"crane/core/messages"
	"crane/core/utils"
	"crane/spout"
	"crane/topology"
	"fmt"
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
func (c *Client) ContactDriver(topoMsg topology.Topology) {
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
	client := NewClient(":" + fmt.Sprintf("%d", utils.DRIVER_PORT))
	if client == nil {
		log.Println("Initialize client failed")
		return
	}
	tm := topology.Topology{}
	tm.Bolts = make([]bolt.BoltInst, 0)
	bm := bolt.BoltInst{
		Name:          "wordcount",
		InstNum:       4,
		PrevTaskNames: []string{"wordgen", "wordgen2"},
		GroupingHint:  utils.GROUPING_BY_SHUFFLE,
		PluginFile:    "wordcount.so",
		PluginSymbol:  "WordCountBolt",
	}
	tm.Bolts = append(tm.Bolts, bm)
	bm2 := bolt.BoltInst{
		Name:          "wordsplit",
		InstNum:       4,
		PrevTaskNames: []string{"wordgen", "wordgen2"},
		GroupingHint:  utils.GROUPING_BY_FIELD,
		FieldIndex:    0,
		PluginFile:    "wordsplit.so",
		PluginSymbol:  "WordSplitBolt",
	}
	tm.Bolts = append(tm.Bolts, bm2)
	sp := spout.NewSpoutInst("wordgen", "wordgen.so", "WordGen", utils.GROUPING_BY_SHUFFLE, 0)
	sp2 := spout.NewSpoutInst("wordgen2", "wordgen.so", "WordGen", utils.GROUPING_BY_SHUFFLE, 0)
	sp.SetInstanceNum(3)
	sp2.SetInstanceNum(3)
	tm.AddSpout(sp)
	tm.AddSpout(sp2)
	client.ContactDriver(tm)
	client.Start()
}
