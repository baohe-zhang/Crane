package messages

import (
	"bufio"
	"crane/core/utils"
	"log"
	"net"
)

// Subscriber who subscribes a certain publisher and receives messages
type Subscriber struct {
	Conn         net.Conn
	PublishBoard chan Message
	Request      chan Message
}

// Factory method to create a new subscriber
// addr is publisher's server address
func NewSubscriber(addr string) *Subscriber {
	sub := &Subscriber{}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		utils.PrintError(err)
		return nil
	}
	sub.Conn = conn
	sub.PublishBoard = make(chan Message, CHANNEL_SIZE)
	sub.Request = make(chan Message, CHANNEL_SIZE)
	return sub
}

// Subscriber would subscribe the messages from publisher
func (sub *Subscriber) ReadMessage() {
	// create new reader instance
	reader := bufio.NewReader(sub.Conn)
	for {
		// read message
		request, _, err := reader.ReadLine()
		if err != nil {
			// stop reading buffer and exit goroutine
			log.Println("Can't read line from socket:", err)
			break
		} else {
			// check request before pushing into channel
			if len(request) == 0 {
				continue
			}
			// Connection Id as the address
			connId := sub.Conn.RemoteAddr().String()
			// push message from subscriber to message channel
			log.Printf("Received message on socket %s", connId)
			sub.PublishBoard <- Message{
				Payload:      request,
				SourceConnId: connId,
			}
		}
	}
}

// Subscriber would also push some message to the publisher as the request message
func (sub *Subscriber) RequestMessage() {
	// get message from channel
	for {
		message := <-sub.Request

		//log.Println("Going to send message on socket %s", message.TargetConnId)

		// send message to targetConn
		writer := bufio.NewWriter(sub.Conn)
		writer.Write(append(message.Payload, '\n'))
		writer.Flush()
	}
}
