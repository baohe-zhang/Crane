package main

import (
	"crane/bolt"
	"crane/core/messages"
	"crane/core/utils"
	"crane/spout"
	"crane/topology"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
)

// Driver, the master node daemon server for scheduling and
// dispaching the spouts or bolts task
type Driver struct {
	Pub             *messages.Publisher
	SupervisorIdMap map[uint32]string
	LockSIM         sync.RWMutex
	TopologyGraph   map[string][]interface{}
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
					d.SupervisorIdMap[uint32(len(d.SupervisorIdMap))] = connId
					d.LockSIM.Unlock()
					log.Println("Supervisor ID Name", content.Name)
				case utils.CONN_NOTIFY:
					content := &messages.ConnNotify{}
					utils.Unmarshal(payload.Content, content)
					if content.Type == messages.CONN_DELETE {
						d.LockSIM.Lock()
						for index, connId_ := range d.SupervisorIdMap {
							if connId_ == connId {
								delete(d.SupervisorIdMap, index)
								delete(d.Pub.Channels, connId)
							}
						}
						d.LockSIM.Unlock()
					}
				case utils.TOPO_SUBMISSION:
					d.Pub.PublishBoard <- messages.Message{
						Payload:      []byte("OK"),
						TargetConnId: connId,
					}
					topo := &topology.Topology{}
					utils.Unmarshal(payload.Content, topo)
					d.BuildTopology(topo)
					d.PrintTopology("None", 0)
					fmt.Println(" ")
					/*for _, bolt := range content.Bolts {*/
					//task := utils.BoltTaskMessage{
					//BoltName:     bolt.Name,
					//PrevBoltAddr: make([]string, 0),
					//GroupingHint: bolt.GroupingHint,
					//FieldIndex:   bolt.FieldIndex,
					//}
					//b, _ := utils.Marshal(utils.BOLT_DISPATCH, task)
					//for i := 0; i < bolt.InstNum; i++ {
					//id := i
					//if i >= len(d.SupervisorIdMap) && i != 0 {
					//id = i % len(d.SupervisorIdMap)
					//} else if len(d.SupervisorIdMap) == 0 {
					//log.Println("No nodes inside the cluster")
					//break
					//}
					//targetId := d.SupervisorIdMap[uint32(id)]
					//log.Println("ConnId Target:", id, uint32(id), targetId)
					//d.Pub.PublishBoard <- messages.Message{
					//Payload:      b,
					//TargetConnId: targetId,
					//}
					//}
					/*}*/
				}
			default:
			}
			d.Pub.RWLock.RUnlock()
		}
	}
}

func (d *Driver) BuildTopology(topo *topology.Topology) {
	d.TopologyGraph = make(map[string][]interface{})
	for _, bolt := range topo.Bolts {
		preVecs := bolt.PrevTaskNames
		for _, vec := range preVecs {
			if d.TopologyGraph[vec] == nil {
				d.TopologyGraph[vec] = make([]interface{}, 0)
			}
			d.TopologyGraph[vec] = append(d.TopologyGraph[vec], bolt)
		}
	}
	for _, spout := range topo.Spouts {
		preVec := "None"
		if d.TopologyGraph[preVec] == nil {
			d.TopologyGraph[preVec] = make([]interface{}, 0)
		}
		d.TopologyGraph[preVec] = append(d.TopologyGraph[preVec], spout)
	}
}

func (d *Driver) PrintTopology(next string, level int) {
	if d.TopologyGraph == nil {
		log.Println("No topology has been built")
		return
	}
	startVecs := d.TopologyGraph[next]
	if startVecs == nil {
		return
	}
	for _, vec := range startVecs {
		fmt.Printf("\n")
		for i := 0; i < level; i++ {
			fmt.Printf("  ")
		}
		if next == "None" {
			fmt.Printf("#%s ", vec.(spout.SpoutInst).Name)
			d.PrintTopology(vec.(spout.SpoutInst).Name, level+1)
		} else {
			fmt.Printf("--- %s ", vec.(bolt.BoltInst).Name)
			d.PrintTopology(vec.(bolt.BoltInst).Name, level+1)
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
