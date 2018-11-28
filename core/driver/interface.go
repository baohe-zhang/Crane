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
	"net"
	"sync"
)

const ()

// Driver, the master node daemon server for scheduling and
// dispaching the spouts or bolts task
type Driver struct {
	Pub             *messages.Publisher
	SupervisorIdMap []string
	LockSIM         sync.RWMutex
	TopologyGraph   map[string][]interface{}
	SpoutMap        map[string]spout.SpoutInst
	BoltMap         map[string]bolt.BoltInst
}

// Factory mode to return the Driver instance
func NewDriver(addr string) *Driver {
	driver := &Driver{}
	driver.Pub = messages.NewPublisher(addr)
	driver.SupervisorIdMap = make([]string, 0)
	driver.SpoutMap = make(map[string]spout.SpoutInst)
	driver.BoltMap = make(map[string]bolt.BoltInst)
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
				// parse the header information
				switch payload.Header.Type {
				// if it is the join request from supervisor
				case utils.JOIN_REQUEST:
					content := &utils.JoinRequest{}
					utils.Unmarshal(payload.Content, content)
					d.LockSIM.Lock()
					d.SupervisorIdMap = append(d.SupervisorIdMap, connId)
					d.LockSIM.Unlock()
					log.Println("Supervisor ID Name", content.Name)
				// if it is the connection notification about the connection pools
				case utils.CONN_NOTIFY:
					content := &messages.ConnNotify{}
					utils.Unmarshal(payload.Content, content)
					if content.Type == messages.CONN_DELETE {
						d.LockSIM.Lock()
						for index, connId_ := range d.SupervisorIdMap {
							if connId_ == connId {
								d.SupervisorIdMap = append(d.SupervisorIdMap[:index], d.SupervisorIdMap[index+1:]...)
								delete(d.Pub.Channels, connId)
							}
						}
						d.LockSIM.Unlock()
					}
				// if it is the topology submitted from the client, which is
				// the application written by the developer
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

// Build the graph topology using vector-edge map
func (d *Driver) BuildTopology(topo *topology.Topology) {
	d.TopologyGraph = make(map[string][]interface{})
	for _, bolt := range topo.Bolts {
		preVecs := bolt.PrevTaskNames
		for _, vec := range preVecs {
			if d.TopologyGraph[vec] == nil {
				d.TopologyGraph[vec] = make([]interface{}, 0)
			}
			d.TopologyGraph[vec] = append(d.TopologyGraph[vec], &bolt)
		}
		bolt.TaskAddrs = make([]string, 0)
	}

	for _, spout := range topo.Spouts {
		preVec := "None"
		if d.TopologyGraph[preVec] == nil {
			d.TopologyGraph[preVec] = make([]interface{}, 0)
		}
		d.TopologyGraph[preVec] = append(d.TopologyGraph[preVec], &spout)
		spout.TaskAddrs = make([]string, 0)
	}

	visited := make(map[string]bool)
	count := 0
	addrs := make(map[int][]interface{})
	d.GenTopologyMessages("None", &visited, &count, &addrs)
	for id, tasks := range addrs {
		targetId := d.SupervisorIdMap[uint32(id)]
		for _, task := range tasks {
			spout, ok := task.(utils.SpoutTaskMessage)
			if ok {
				task := utils.SpoutTaskMessage{
					Name:         spout.Name,
					PrevBoltAddr: d.SpoutMap[spout.Name].TaskAddrs,
					GroupingHint: spout.GroupingHint,
					FieldIndex:   spout.FieldIndex,
				}
			} else {

			}
		}
	}
}

// Generate Topology Messages for each bolt or spout instance
func (d *Driver) GenTopologyMessages(next string, visited *map[string]bool, count *int, addrs *map[int][]interface{}) {
	if d.TopologyGraph == nil {
		log.Println("No topology has been built")
		return
	}

	startVecs := d.TopologyGraph[next]
	if startVecs == nil {
		return
	}

	for _, vec := range startVecs {
		if next == "None" {
			spout := vec.(*spout.SpoutInst)
			if (*visited)[(*spout).Name] == true {
				continue
			}

			fmt.Printf("#%s ", (*spout).Name)
			(*visited)[(*spout).Name] = true
			for i := 0; i < (*spout).InstNum; i++ {
				id := (*count) % len(d.SupervisorIdMap)
				if (*addrs)[id] == nil {
					(*addrs)[id] = make([]interface{}, 0)
				}
				targetId := d.SupervisorIdMap[uint32(id)]
				host, _, _ := net.SplitHostPort(targetId)
				(*spout).TaskAddrs = append((*spout).TaskAddrs, host+":"+fmt.Sprintf("%d", utils.CONTRACTOR_BASE_PORT+len((*addrs)[id])))
				(*addrs)[id] = append((*addrs)[id], spout)
				(*count)++
			}
			d.GenTopologyMessages(spout.Name, visited, count, addrs)
		} else {
			bolt := vec.(*bolt.BoltInst)
			if (*visited)[(*bolt).Name] == true {
				continue
			}
			(*visited)[(*bolt).Name] = true
			fmt.Printf("#%s ", (*bolt).Name)
			for i := 0; i < (*bolt).InstNum; i++ {
				id := (*count) % len(d.SupervisorIdMap)
				if (*addrs)[id] == nil {
					(*addrs)[id] = make([]interface{}, 0)
				}
				targetId := d.SupervisorIdMap[uint32(id)]
				host, _, _ := net.SplitHostPort(targetId)
				(*bolt).TaskAddrs = append((*bolt).TaskAddrs, host+":"+fmt.Sprintf("%d", utils.CONTRACTOR_BASE_PORT+len((*addrs)[id])))
				(*addrs)[id] = append((*addrs)[id], bolt)
				(*count)++
			}
			d.GenTopologyMessages(bolt.Name, visited, count, addrs)
		}
	}
}

// Output the topology in the std out
func (d *Driver) PrintTopology(next string, level int) {
	spoutMap := make(map[string]spout.SpoutInst)
	boltMap := make(map[string]bolt.BoltInst)
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
			fmt.Printf("#%s ", vec.(*spout.SpoutInst).Name)
			d.spoutMap[vec.(*spout.SpoutInst).Name] = (*vec.(*spout.SpoutInst))
			fmt.Println(vec.(*spout.SpoutInst).TaskAddrs)
			d.PrintTopology(vec.(*spout.SpoutInst).Name, level+1)
		} else {
			fmt.Printf("--- %s ", vec.(*bolt.BoltInst).Name)
			d.boltMap[vec.(*bolt.BoltInst).Name] = (*vec.(*bolt.BoltInst))
			fmt.Println(vec.(*bolt.BoltInst).TaskAddrs)
			d.PrintTopology(vec.(*bolt.BoltInst).Name, level+1)
		}
	}
}

func (d *Driver) Hashcode(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	hashcode := h.Sum32()
	hashcode = (hashcode + 5) >> 5 % uint32(len(d.SupervisorIdMap))
	return hashcode
}

func main() {
	driver := NewDriver(":" + fmt.Sprintf("%d", utils.DRIVER_PORT))
	driver.StartDaemon()
}
