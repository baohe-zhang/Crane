package main

import (
	"crane/core/messages"
	"crane/core/utils"
	"log"
	"sync"
)

// Driver, the master node daemon server for scheduling and
// dispaching the spouts or bolts task
type Driver struct {
	Pub             *messages.Publisher
	SupervisorIdMap map[uint32]string
	LockSIM         sync.RWMutex
}

// Factory mode to return the Driver instance
func NewDriver(addr string) *Driver {
	driver := &Driver{}
	driver.Pub = messages.NewPublisher(addr)
	driver.SupervisorIdMap = make(map[uint32]string)
	return driver
}

// Start the Driver daemon
func (d *Driver) StartDaemon() {
	go d.Pub.AcceptConns()
	go d.Pub.PublishMessage(d.Pub.PublishBoard)
	for {
		for connId, channel := range d.Pub.Channels {
			d.Pub.RWLock.RLock()
			select {
			case supervisorMsg := <-channel:
				log.Printf("Message from %s: %s\n", connId, supervisorMsg.Payload)
				payload := utils.CheckType(supervisorMsg.Payload)
				log.Printf("Receiving %s request form %s\n", payload.Header.Type, connId)
				switch payload.Header.Type {
				case utils.JOIN_REQUEST:
					content := &utils.JoinRequest{}
					utils.Unmarshal(payload.Content, content)
					d.LockSIM.Lock()
					d.SupervisorIdMap[uint32(d.Pub.Pool.Size()-1)] = connId
					d.LockSIM.Unlock()
					log.Println("Supervisor ID Name", content.Name)

				}
			default:
			}
			d.Pub.RWLock.RUnlock()
		}
	}
}

func (d *Driver) Hashcode(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	hashcode := h.Sum32()
	hashcode = (hashcode + 5) >> 5 % uint32(d.Pub.Pool.Size())
	return hashcode
}

func main() {
	driver := NewDriver(":5001")
	driver.StartDaemon()
}
