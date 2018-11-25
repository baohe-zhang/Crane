package main 

import (
	"fmt"
	"sync"
	// "time"
	// "math/rand"
	"encoding/json"
	"net"
	"crane/core/messages"
)

const (
	BUFLEN = 100
	BUFFSIZE = 1024
)

type ProcessorFunc func(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error

type Contractor struct {
	numWorkers int
	workers []*Worker
	tuples chan []interface{}
	results chan []interface{}
	port string
	pub *messages.Publisher
	subs []*messages.Subscriber
	preGrouping string
	preField int
	sucGrouping string
	sucField int
	sucIndexMap map[int]string
	rwmutex sync.RWMutex
	wg sync.WaitGroup
}

type Worker struct {
	id int
	available bool
	results chan []interface{}
	procFunc ProcessorFunc
	variables []interface{}
}

func NewContractor(numWorkers int, procFunc ProcessorFunc, port string, subAddrs []string, 
					preGrouping string, preField int, sucGrouping string, sucField int) *Contractor {
	tuples := make(chan []interface{}, BUFLEN)
	results := make(chan []interface{}, BUFLEN)

	// Create workers
	workers := make([]*Worker, 0)
	for i := 0; i < numWorkers; i++ {
		variables := make([]interface{}, 0) // Store bolt's global variables
		worker := &Worker {
			id: i,
			available: true,
			results: results,
			procFunc: procFunc,
			variables: variables,
		}
		workers = append(workers, worker)
	}

	// Create publisher and subscribers
	publisher := messages.NewPublisher(":"+port)
	subscribers := make([]*messages.Subscriber, 0)
	for _, subAddr := range subAddrs {
		subscribers = append(subscribers, messages.NewSubscriber(subAddr))
	}

	// Record the index of successor
	sucIndexMap := make(map[int]string)

	c := &Contractor{
		numWorkers: numWorkers,
		workers: workers,
		tuples: tuples,
		results: results,
		port: port,
		pub: publisher,
		subs: subscribers,
		preGrouping: preGrouping,
		preField: preField,
		sucGrouping: sucGrouping,
		sucField: sucField,
		sucIndexMap: sucIndexMap,
	}

	return c
}

func (c *Contractor) Start() {
	defer close(c.tuples)
	defer close(c.results)

	fmt.Println("contractor start")

	// Start subscriber
	for _, sub := range c.subs {
		go sub.ReadMessage()
	}
	// Start publisher
	go c.pub.AcceptConns()
	go c.pub.PublishMessage(c.pub.PublishBoard)

	c.wg.Add(1)
	go c.buildSucIndexMap()
	c.wg.Wait()

	go c.receiveTuple()
	go c.distributeTuple()
	go c.outputTuple()

	c.wg.Add(1)
	c.wg.Wait()
}

func (c *Contractor) receiveTuple() {
	for _, sub := range c.subs {
		go func() {
			for msg := range sub.PublishBoard {
				var tuple []interface{}
				json.Unmarshal(msg.Payload, &tuple)
				c.tuples <- tuple
			}
		}()
	}
}

func (c *Contractor) distributeTuple() {
	switch c.preGrouping {
	case "shuffle":
		for tuple := range c.tuples {
			processed := false
			for !processed {
				// Round-Robin distribute
				for _, worker := range c.workers {
					if worker.available {
						go worker.processTuple(tuple)
						processed = true
						break
					}
				}
			}
		}
	case "byFields":
		for tuple := range c.tuples {
			wid := hash(tuple[c.preField]) % c.numWorkers
			worker := c.workers[wid]
			processed := false
			for !processed {
				if worker.available {
					go worker.processTuple(tuple)
					processed = true
					break
				}
			}
		}
	default:
	}
}

func (w *Worker) processTuple(tuple []interface{}) {
	w.available = false

	// fmt.Printf("worker (%d) process tuple (%v)\n", w.id, tuple)
	var result []interface{}
	w.procFunc(tuple, &result, &w.variables)
	fmt.Printf("worker %d output tuple (%v)\n", w.id, result)
	w.results <- result

	w.available = true
}

func (c *Contractor) outputTuple() {
	switch c.sucGrouping {
	case "shuffle":
		count := 0
		for result := range c.results {
			bin, _ := json.Marshal(result)
			sucid := count % len(c.sucIndexMap)
			c.rwmutex.RLock()
			sucConnId := c.sucIndexMap[sucid]
			c.rwmutex.RUnlock()
			c.pub.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: sucConnId,
			}
			count++
		}
	case "byFields":
		for result := range c.results {
			bin, _ := json.Marshal(result)
			sucid := hash(result[c.sucField]) % len(c.sucIndexMap)
			c.rwmutex.RLock()
			sucConnId := c.sucIndexMap[sucid]
			c.rwmutex.RUnlock()
			c.pub.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: sucConnId,
			}
		}
	case "all":
		for result := range c.results {
			bin, _ := json.Marshal(result)
			c.pub.Pool.Range(func(id string, conn net.Conn) {
				c.pub.PublishBoard <- messages.Message{
					Payload: bin,
					TargetConnId: id,
				}
			})
		}
	default:
	}
}

func (c *Contractor) buildSucIndexMap() {
	c.pub.Pool.Range(func(id string, conn net.Conn) {
		c.rwmutex.Lock()
		c.sucIndexMap[len(c.sucIndexMap)] = id
		c.rwmutex.Unlock()
	})
	c.wg.Done()
}

func processorFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Bolt's global variables
	var countMap map[string]int
	if (len(*variables) == 0) {
		// Initialize varibales
		countMap = make(map[string]int)
		*variables = append(*variables, countMap)
	} else {
		countMap = (*variables)[0].(map[string]int)
	}

	// Bolt's process logic
	word := tuple[0].(string)
	count, ok := countMap[word]
	if !ok {
		count = 0
	}
	count++
	countMap[word] = count
	*result = []interface{}{word, count}

	return nil
}


func main() {
	contractor := NewContractor(10, processorFunc, "5000", []string{"127.0.0.1:5001"}, "byFields", 0, "all", 0)
	contractor.Start()
}






