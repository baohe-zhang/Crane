package spoutworker 

import (
	"fmt"
	"sync"
	"time"
	"encoding/json"
	"net"
	"crane/core/messages"
	"crane/core/utils"
)

const (
	BUFLEN = 100
)

type SpoutWorker struct {
	ProcFuncName string
	procFunc func([]interface{}, *[]interface{}, *[]interface{}) error
	port string
	tuples chan []interface{}
	variables []interface{}
	publisher *messages.Publisher
	sucGrouping string
	sucField int
	sucIndexMap map[int]string
	rwmutex sync.RWMutex
	wg sync.WaitGroup
}

func NewSpoutWorker(pluginFilename string, procFuncName string, port string, sucGrouping string, sucField int) *SpoutWorker {
	procFunc := utils.LookupProcFunc(pluginFilename, procFuncName)

	tuples := make(chan []interface{}, BUFLEN)
	variables := make([]interface{}, 0) // Store spout's global variables

	// Create publisher
	var publisher *messages.Publisher

	// A map to record the index of successor
	sucIndexMap := make(map[int]string)

	sw := &SpoutWorker{
		ProcFuncName: procFuncName,
		procFunc: procFunc,
		port: port,
		tuples: tuples,
		variables: variables,
		publisher: publisher,
		sucGrouping: sucGrouping,
		sucField: sucField,
		sucIndexMap: sucIndexMap,
	}

	return sw
}

func (sw *SpoutWorker) Start() {
	defer close(sw.tuples)

	fmt.Printf("spout worker %s start\n", sw.ProcFuncName)

	// Start publisher
	sw.publisher = messages.NewPublisher(":"+sw.port)
	go sw.publisher.AcceptConns()
	go sw.publisher.PublishMessage(sw.publisher.PublishBoard)
	time.Sleep(10 * time.Second) // Wait for all subscribers to join 

	sw.buildSucIndexMap()

	go sw.receiveTuple()
	go sw.outputTuple()

	sw.wg.Add(1)
	sw.wg.Wait()
}

// Receive tuple from input stream
func (sw *SpoutWorker) receiveTuple() {
	for {
		var empty []interface{}
		var tuple []interface{}
		err :=  sw.procFunc(empty, &tuple, &sw.variables)
		if (err != nil) {
			continue
		} 
		sw.tuples <- tuple
	}	
}

func (sw *SpoutWorker) outputTuple() {
	switch sw.sucGrouping {
	case "shuffle":
		count := 0
		for tuple := range sw.tuples {
			bin, _ := json.Marshal(tuple)
			sucid := count % len(sw.sucIndexMap)
			sw.rwmutex.RLock()
			sucConnId := sw.sucIndexMap[sucid]
			sw.rwmutex.RUnlock()
			sw.publisher.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: sucConnId,
			}
			count++
		}
	case "byFields":
		for tuple := range sw.tuples {
			bin, _ := json.Marshal(tuple)
			sucid := utils.Hash(tuple[sw.sucField]) % len(sw.sucIndexMap)
			sw.rwmutex.RLock()
			sucConnId := sw.sucIndexMap[sucid]
			sw.rwmutex.RUnlock()
			sw.publisher.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: sucConnId,
			}
		}
	case "all":
		for tuple := range sw.tuples {
			bin, _ := json.Marshal(tuple)
			sw.publisher.Pool.Range(func(id string, conn net.Conn) {
				sw.publisher.PublishBoard <- messages.Message{
					Payload: bin,
					TargetConnId: id,
				}
			})
		}
	default:
	}
}

func (sw *SpoutWorker) buildSucIndexMap() {
	sw.publisher.Pool.Range(func(id string, conn net.Conn) {
		sw.rwmutex.Lock()
		sw.sucIndexMap[len(sw.sucIndexMap)] = id
		sw.rwmutex.Unlock()
	})
}


// func main() {
// 	spoutWorker := NewSpoutWorker("NextTuple", "5000", "byFields", 0)
// 	go spoutWorker.Start()

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	wg.Wait()
// }