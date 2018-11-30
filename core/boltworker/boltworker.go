package boltworker 

import (
	"fmt"
	"sync"
	"time"
	"encoding/json"
	"net"
	"crane/core/messages"
	"crane/core/utils"
	"os"
	"io/ioutil"
)

const (
	BUFLEN = 100
	BUFFSIZE = 1024
)

type BoltWorker struct {
	ProcFuncName string
	numWorkers int
	executors []*Executor
	tuples chan []interface{}
	results chan []interface{}
	port string
	subAddrs []string
	publisher *messages.Publisher
	subscribers []*messages.Subscriber
	preGrouping string
	preField int
	sucGrouping string
	sucField int
	sucIndexMap map[int]string
	rwmutex sync.RWMutex
	wg sync.WaitGroup
}

type Executor struct {
	id int
	available bool
	results chan []interface{}
	procFunc func([]interface{}, *[]interface{}, *[]interface{}) error
	variables []interface{}
}

func NewBoltWorker(numWorkers int, pluginFilename string, procFuncName string, port string, subAddrs []string, 
					preGrouping string, preField int, 
					sucGrouping string, sucField int) *BoltWorker {
	tuples := make(chan []interface{}, BUFLEN)
	results := make(chan []interface{}, BUFLEN)

	// Lookup ProcFunc
	procFunc := utils.LookupProcFunc(pluginFilename, procFuncName)

	// Create executors
	executors := make([]*Executor, 0)
	for i := 0; i < numWorkers; i++ {
		variables := make([]interface{}, 0) // Store bolt's global variables
		executor := &Executor {
			id: i,
			available: true,
			results: results,
			procFunc: procFunc,
			variables: variables,
		}
		executors = append(executors, executor)
	}

	// Create publisher and subscribers
	var publisher *messages.Publisher
	subscribers := make([]*messages.Subscriber, 0)

	// Record the index of successor
	sucIndexMap := make(map[int]string)

	bw := &BoltWorker{
		ProcFuncName: procFuncName, 
		numWorkers: numWorkers,
		executors: executors,
		tuples: tuples,
		results: results,
		port: port,
		subAddrs: subAddrs,
		publisher: publisher,
		subscribers: subscribers,
		preGrouping: preGrouping,
		preField: preField,
		sucGrouping: sucGrouping,
		sucField: sucField,
		sucIndexMap: sucIndexMap,
	}

	return bw
}

func (bw *BoltWorker) Start() {
	defer close(bw.tuples)
	defer close(bw.results)

	fmt.Printf("bolt worker %s start\n", bw.ProcFuncName)

	// Start publisher
	bw.publisher = messages.NewPublisher(":"+bw.port)
	go bw.publisher.AcceptConns()
	go bw.publisher.PublishMessage(bw.publisher.PublishBoard)
	time.Sleep(2 * time.Second) // Wait for all boltWorkers' publisher established
	
	// Start subscribers
	for _, subAddr := range bw.subAddrs {
		subscriber := messages.NewSubscriber(subAddr)
		bw.subscribers = append(bw.subscribers, subscriber)
		go subscriber.ReadMessage()
	}
	time.Sleep(2 * time.Second) // Wait for all subscriber established

	bw.buildSucIndexMap()

	go bw.receiveTuple()
	go bw.distributeTuple()
	go bw.outputTuple()

	//Test
	time.Sleep(40 * time.Second)
	fmt.Println("start serialize")
	bw.SerializeVariables()
	time.Sleep(5 * time.Second)
	fmt.Println("start deserialize")
	bw.DeserializeVariables()
	// End Test

	bw.wg.Add(1)
	bw.wg.Wait()
}

func (bw *BoltWorker) receiveTuple() {
	for _, subscriber := range bw.subscribers {
		go func() {
			for msg := range subscriber.PublishBoard {
				var tuple []interface{}
				json.Unmarshal(msg.Payload, &tuple)
				bw.tuples <- tuple
			}
		}()
	}
}

func (bw *BoltWorker) distributeTuple() {
	// TODO: only need group by field
	switch bw.preGrouping = utils.GROUPING_BY_FIELD; bw.preGrouping {
	case utils.GROUPING_BY_SHUFFLE:
		for tuple := range bw.tuples {
			processed := false
			for !processed {
				// Round-Robin distribute
				for _, executor := range bw.executors {
					if executor.available {
						go executor.processTuple(tuple)
						processed = true
						break
					}
				}
			}
		}
	case utils.GROUPING_BY_FIELD:
		for tuple := range bw.tuples {
			execid := utils.Hash(tuple[bw.preField]) % bw.numWorkers
			executor := bw.executors[execid]
			processed := false
			for !processed {
				if executor.available {
					go executor.processTuple(tuple)
					processed = true
					break
				}
			}
		}
	default:
	}
}

func (e *Executor) processTuple(tuple []interface{}) {
	e.available = false

	// fmt.Printf("executor (%d) process tuple (%v)\n", e.id, tuple)
	var result []interface{}
	e.procFunc(tuple, &result, &e.variables)
	// fmt.Printf("executor %d output tuple (%v)\n", e.id, result)
	e.results <- result

	e.available = true
}

func (bw *BoltWorker) outputTuple() {
	switch bw.sucGrouping {
	case utils.GROUPING_BY_SHUFFLE:
		count := 0
		for result := range bw.results {
			bin, _ := json.Marshal(result)
			sucid := count % len(bw.sucIndexMap)
			bw.rwmutex.RLock()
			sucConnId := bw.sucIndexMap[sucid]
			bw.rwmutex.RUnlock()
			bw.publisher.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: sucConnId,
			}
			count++
		}
	case utils.GROUPING_BY_FIELD:
		for result := range bw.results {
			bin, _ := json.Marshal(result)
			sucid := utils.Hash(result[bw.sucField]) % len(bw.sucIndexMap)
			bw.rwmutex.RLock()
			sucConnId := bw.sucIndexMap[sucid]
			bw.rwmutex.RUnlock()
			bw.publisher.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: sucConnId,
			}
		}
	case utils.GROUPING_BY_ALL:
		for result := range bw.results {
			bin, _ := json.Marshal(result)
			bw.publisher.Pool.Range(func(id string, conn net.Conn) {
				bw.publisher.PublishBoard <- messages.Message{
					Payload: bin,
					TargetConnId: id,
				}
			})
		}
	default:
	}
}

// Build a successor boltworker's [index : netaddr] map
func (bw *BoltWorker) buildSucIndexMap() {
	bw.publisher.Pool.Range(func(id string, conn net.Conn) {
		bw.rwmutex.Lock()
		bw.sucIndexMap[len(bw.sucIndexMap)] = id
		bw.rwmutex.Unlock()
	})
}

// Serialize and store executors' variables into local file
func (bw BoltWorker) SerializeVariables() {
	// Merge all executors' variables
	var bins []interface{}
	for _, executor := range bw.executors {
		bins = append(bins, executor.variables)
	}

	// Create file to store
	file, err := os.Create(bw.ProcFuncName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	// Store bins's binary value into the file
	b, _ := json.Marshal(bins)
	file.Write(b)
}

// Deserialize executors' variables from local file
func (bw BoltWorker) DeserializeVariables() {
	// Open the local file that stores the variables' binary value
	b, err := ioutil.ReadFile(bw.ProcFuncName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Unmarshal the binary value
	var bins []interface{}
	json.Unmarshal(b, &bins)

	// Deserialize to get each executor's variables
	for index, bin := range bins {
		bw.executors[index].variables = bin.([]interface{})
	}

	// Test log
	for _, executor := range bw.executors {
		fmt.Println(executor.variables)
	}
}


// func main() {
// 	boltWorker_1 := NewBoltWorker(10, "ProcFunc", "5001", []string{"127.0.0.1:5000"}, "byFields", 0, "shuffle", 0)
// 	boltWorker_2 := NewBoltWorker(10, "ProcFunc", "5002", []string{"127.0.0.1:5001"}, "byFields", 0, "shuffle", 0)
// 	boltWorker_3 := NewBoltWorker(10, "ProcFunc", "5003", []string{"127.0.0.1:5002"}, "byFields", 0, "all", 0)

// 	go boltWorker_1.Start()
// 	go boltWorker_2.Start()
// 	go boltWorker_3.Start()

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	wg.Wait()
// }





