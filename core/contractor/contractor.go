package main 

import (
	"fmt"
	"sync"
	// "time"
	// "math/rand"
	"encoding/json"
	"net"
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
}

type Worker struct {
	id int
	available bool
	results chan []interface{}
	procFunc ProcessorFunc
	variables []interface{}
}

func NewContractor(numWorkers int, procFunc ProcessorFunc, port string) *Contractor {
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

	c := &Contractor{
		numWorkers: numWorkers,
		workers: workers,
		tuples: tuples,
		results: results,
		port: port,
	}

	return c
}

func (c *Contractor) Start() {
	defer close(c.tuples)
	defer close(c.results)

	fmt.Println("contractor start")
	go c.setupListener()
	go c.distributeTuple()
	go c.outputTuple()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func (c *Contractor) setupListener() {
	listener, err := net.Listen("tcp", ":"+c.port)
	fmt.Println("listening on port " + c.port)
	if err != nil {
		fmt.Println(err.Error())
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err.Error())
		}
		go c.receiveTuple(conn)
	}
}

func (c *Contractor) receiveTuple(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, BUFFSIZE)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err.Error())
		}
		var tuple []interface{}
		json.Unmarshal(buf[:n], &tuple)
		c.tuples <- tuple
	}

	// words := []string{"one", "two", "three", "four", "five", "six"}
	// for i := 0; i < 1000; i++ {
	// 	tuple := make([]interface{}, 0)
	// 	tuple = append(tuple, words[rand.Intn(len(words))])
	// 	c.tuples <- tuple
	// 	time.Sleep(10 * time.Millisecond)
	// }
}

func (c *Contractor) distributeTuple() {
	switch grouping := "fields"; grouping {
	case "shuffle":
		for {
			select {
			case tuple := <-c.tuples:
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
			default:
			}
		}

	case "fields":
		for {
			select {
			case tuple := <-c.tuples:
				// distribute by fields
				wid := hash(tuple[0]) % c.numWorkers
				worker := c.workers[wid]
				processed := false
				for !processed {
					if worker.available {
						go worker.processTuple(tuple)
						processed = true
						break
					}
				}
			default:
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
	fmt.Printf("worker (%d) output tuple (%v)\n", w.id, result)
	w.results <- result

	w.available = true
}

func (c *Contractor) outputTuple() {
	for {
		select {
		case <- c.results:
			// fmt.Printf("output tuple (%v)\n", result)

		default:
		}
	}
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
	contractor := NewContractor(10, processorFunc, "5000")
	contractor.Start()
}






