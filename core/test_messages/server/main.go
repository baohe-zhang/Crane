package main

import (
	"crane/core/messages"
	"crane/core/utils"
	"fmt"
	"net"
	"time"
)

type Topology struct {
	Spout string
	Bolt  string
}

func main() {
	fmt.Println("vim-go")
	publisher := messages.NewPublisher(":5001")
	go publisher.AcceptConns()
	go publisher.PublishMessage(publisher.PublishBoard)
	topology := Topology{"split", "words"}
	for {
		publisher.Pool.Range(func(id string, conn net.Conn) {
			//msg := <-publisher.Channels[id]
			b, err := utils.Marshal("Topology", topology)
			if err != nil {
				fmt.Println(err)
				return
			}
			publisher.PublishBoard <- messages.Message{
				Payload:      b,
				TargetConnId: id,
			}
		})
		time.Sleep(1 * time.Second)

	}
}
