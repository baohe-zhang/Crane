package main 

import (
	// "fmt"
	"time"
	// "math/rand"
	"encoding/json"
	"sync"
	"net"
	"crane/core/messages"
)

const (
	BUFLEN = 100
)

var tuples chan []interface{}
var pub *messages.Publisher

func sendTuple() {
	for tuple := range tuples {
		bin, _ := json.Marshal(tuple)
		pub.Pool.Range(func(id string, conn net.Conn) {
			pub.PublishBoard <- messages.Message{
				Payload: bin,
				TargetConnId: id,
			}
		})
	}
}

func nextTuple() {
	for i := 0; i < 101; i++ {
		tuple := make([]interface{}, 0)
		tuple = append(tuple, i)
		tuples <- tuple
		time.Sleep(100 * time.Millisecond)
	}
}

// func nextTuple() {
// 	words := []string{"one", "two", "three", "four", "five", "six"}

// 	for i := 0; i < 1200; i++ {
// 		tuple := make([]interface{}, 0)
// 		tuple = append(tuple, words[i%len(words)])
// 		tuples <- tuple
// 		time.Sleep(10 * time.Millisecond)
// 	}

// 	time.Sleep(5 * time.Second)

// 	for i := 0; i < 1200; i++ {
// 		tuple := make([]interface{}, 0)
// 		tuple = append(tuple, words[i%len(words)])
// 		tuples <- tuple
// 		time.Sleep(10 * time.Millisecond)
// 	}
// }

func main() {
	tuples = make(chan []interface{}, BUFLEN)
	defer close(tuples)

	pub = messages.NewPublisher(":5000")
	go pub.AcceptConns()
	go pub.PublishMessage(pub.PublishBoard)
	go sendTuple()

	time.Sleep(10 * time.Second)
	go nextTuple()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}





