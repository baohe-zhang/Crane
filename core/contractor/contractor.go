package main 

import (
	"fmt"
	"sync"
	"time"
	"math/rand"
)

const (
	BUFLEN = 100
)

type ProcessorFunc func(tuple interface{}, result *interface{}, variables []interface{}) error

type Contractor struct {
	numWorkers int
	workers []*Worker
	tuples chan interface{}
	results chan interface{}
	procFunc ProcessorFunc
}

type Worker struct {
	id int
	available bool
	results chan interface{}
	procFunc ProcessorFunc
	variables []interface{}
}

func NewContractor(numWorkers int, procFunc ProcessorFunc, _variables []interface{}) *Contractor {
	tuples := make(chan interface{}, BUFLEN)
	results := make(chan interface{}, BUFLEN)

	workers := make([]*Worker, 0)
	for i := 0; i < numWorkers; i++ {
		// Copy variables for each worker
		variables := make([]interface{}, 0)
		// copy(variables, _variables)
		variables = append(variables[:], _variables[:]...)

		// Create worker
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
		procFunc: procFunc,
	}

	return c
}

func (c *Contractor) Start() {
	defer close(c.tuples)
	defer close(c.results)

	fmt.Println("contractor start")
	go c.receiveTuple()
	go c.distributeTuple()
	go c.outputTuple()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func (c *Contractor) receiveTuple() {
	words := []string{"one", "two", "three", "four", "five", "six"}
	for i := 0; i < 20; i++ {
		c.tuples <- words[rand.Intn(len(words))]
		time.Sleep(1000 * time.Millisecond)
	}
}

func (c *Contractor) distributeTuple() {
	for {
		select {
		case tuple := <-c.tuples:
			processed := false
			for !processed {
				for _, worker := range c.workers {
					if worker.available {
						processed = true
						go worker.processTuple(tuple)
						break
					}
				}
			}

		default:
		}
	}
}

func (c *Contractor) outputTuple() {
	for {
		select {
		case result := <- c.results:
			fmt.Printf("output tuple (%v)\n", result)

		default:
		}
	}
}

func (w *Worker) processTuple(tuple interface{}) {
	w.available = false

	// fmt.Printf("worker (%d) receives tuple (%v)\n", w.id, tuple)
	var result interface{}
	w.procFunc(tuple, &result, w.variables)
	fmt.Printf("worker id: %d, map: %p\n", w.id, w.variables[0])
	time.Sleep(4000 * time.Millisecond) // Simulate process delay
	w.results <- result

	w.available = true
}


func processorFunc(tuple interface{}, result *interface{}, variables []interface{}) error {
	// Wrap worker-scope variables
	variable := variables[0]
	countMap, _ := variable.(map[string]int)

	word := tuple.(string)
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
	// Wrap variables map
	variables := make([]interface{}, 0)
	countMap := make(map[string]int)
	variables = append(variables, countMap)

	contractor := NewContractor(4, processorFunc, variables)
	contractor.Start()
}







