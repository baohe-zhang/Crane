package main

import (
	"crane/core/messages"
	"crane/core/utils"
	"fmt"
)

type Topology struct {
	Spout string
	Bolt  string
}

func main() {
	fmt.Println("vim-go")
	subscriber := messages.NewSubscriber(":5001")
	go subscriber.ReadMessage()
	for {
		msg := <-subscriber.PublishBoard
		payload := utils.CheckType(msg.Payload)
		switch payload.Header.Type {
		case "Topology":
			content := &Topology{}
			utils.Unmarshal(payload.Content, content)
			fmt.Printf("Receiving publishing message %s\n", payload.Header.Type)
			fmt.Println("Receiving " + content.Spout)
		}
	}
}
