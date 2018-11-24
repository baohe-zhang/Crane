package messages

import (
	"bufio"
	"crane/core/utils"
	"log"
	"net"
	"sync"
)

const (
	CHANNEL_SIZE = 100
	CONN_DELETE  = "delete"
)

// The notify of message connection itselt
type ConnNotify struct {
	Type string
}

// Publisher who publishes messages to subscribers and meanwhile receive messages
type Publisher struct {
	Listener     net.Listener
	Pool         *ConnPool
	Channels     map[string]chan Message
	PublishBoard chan Message
	RWLock       sync.RWMutex
}

// Factory method to create a new publisher
// addr is publisher's server address
func NewPublisher(addr string) *Publisher {
	pub := &Publisher{}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		utils.PrintError(err)
		return nil
	}
	pub.Listener = listener
	pub.PublishBoard = make(chan Message, CHANNEL_SIZE)
	pub.Pool = NewConnPool()
	pub.Channels = make(map[string]chan Message)
	return pub
}

// Publisher would publish messages from message channel to target connection
func (pub *Publisher) PublishMessage(msgChan chan Message) {
	// get message from channel
	for {
		message := <-msgChan

		log.Printf("Going to send message '%s' on socket %s", string(message.Payload), message.TargetConnId)

		// send message to targetConn
		targetConn := pub.Pool.Get(message.TargetConnId)
		writer := bufio.NewWriter(targetConn)
		writer.Write(append(message.Payload, '\n'))
		writer.Flush()
	}
}

// Publisher would wait new connection on the listening socket
func (pub *Publisher) AcceptConns() {
	// forever loop to accept new connections
	for {
		// accept connection
		conn, err := pub.Listener.Accept()
		if err != nil {
			log.Fatalln("Fail to accept connection. ", err)
			break
		}

		connId := conn.RemoteAddr().String()
		log.Println(connId)
		connChan := make(chan Message, CHANNEL_SIZE)
		pub.RWLock.Lock()
		pub.Channels[connId] = connChan
		pub.RWLock.Unlock()

		// add connection to pool
		pub.Pool.Insert(connId, conn)

		// log about connection status
		log.Printf("Connection accepted. Connections in pool: %d", pub.Pool.Size())

		// handle request
		go pub.WaitMessage(
			connChan,
			connId,
		)
	}

}

// Publisher would wait new message from subscribers comming
func (pub *Publisher) WaitMessage(msgChan chan Message, connId string) {
	// create new reader instance
	reader := bufio.NewReader(pub.Pool.Get(connId))
	for {
		// read request
		request, _, err := reader.ReadLine()
		if err != nil {
			// stop reading buffer and exit goroutine
			pub.Pool.Delete(connId)
			pub.RWLock.Lock()
			b, _ := utils.Marshal(utils.CONN_NOTIFY, ConnNotify{Type: CONN_DELETE})
			msgChan <- Message{
				Payload:      b,
				SourceConnId: connId,
			}
			pub.RWLock.Unlock()

			log.Printf("Can't read line from socket: %s. Connections in pool: %d", err, pub.Pool.Size())
			break
		} else {
			// check request before pushing into channel
			if len(request) == 0 {
				continue
			}
			// push request to message channel
			pub.RWLock.Lock()
			msgChan <- Message{
				Payload:      request,
				SourceConnId: connId,
			}
			pub.RWLock.Unlock()
		}
	}
}
