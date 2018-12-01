package spoutworker 

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
	"strings"
)

const (
	BUFLEN = 100
)

type SpoutWorker struct {
	Name string
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
	SupervisorC chan string
	WorkerC chan string
	suspend bool
}

func NewSpoutWorker(name string, pluginFilename string, pluginSymbol string, port string, 
					sucGrouping string, sucField int, supervisorC chan string, workerC chan string) *SpoutWorker {

	procFunc := utils.LookupProcFunc(pluginFilename, pluginSymbol)

	tuples := make(chan []interface{}, BUFLEN)
	variables := make([]interface{}, 0) // Store spout's global variables

	// Create publisher
	var publisher *messages.Publisher

	// A map to record the index of successor
	sucIndexMap := make(map[int]string)

	sw := &SpoutWorker{
		Name: name,
		procFunc: procFunc,
		port: port,
		tuples: tuples,
		variables: variables,
		publisher: publisher,
		sucGrouping: sucGrouping,
		sucField: sucField,
		sucIndexMap: sucIndexMap,
		SupervisorC: supervisorC,
		WorkerC: workerC,
		suspend: false,
	}

	return sw
}

func (sw *SpoutWorker) Start() {
	// defer close(sw.tuples)

	fmt.Printf("spout worker %s start\n", sw.Name)

	// Start channel with supervisor
	go sw.TalkWithSupervisor()

	// Start publisher
	sw.publisher = messages.NewPublisher(":"+sw.port)
	go sw.publisher.AcceptConns()
	go sw.publisher.PublishMessage(sw.publisher.PublishBoard)
	time.Sleep(4 * time.Second) // Wait for all subscribers to join 

	sw.buildSucIndexMap()

	go sw.receiveTuple()
	go sw.outputTuple()

	sw.wg.Add(1)
	sw.wg.Wait()
}

// Receive tuple from input stream
func (sw *SpoutWorker) receiveTuple() {
	for {
		if (sw.suspend == true) {
			var wg sync.WaitGroup
			wg.Add(1)
			wg.Wait()
		}
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
	case utils.GROUPING_BY_SHUFFLE:
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
	case utils.GROUPING_BY_FIELD:
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
	case utils.GROUPING_BY_ALL:
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

// Serialize and store variables into local file
func (sw *SpoutWorker) SerializeVariables(version string) {
	// Create file to store
	file, err := os.Create(sw.Name + "-" + version)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	// Store variable's binary value into the file
	b, _ := json.Marshal(sw.variables)
	file.Write(b)
}

// Deserialize variables from local file
func (sw *SpoutWorker) DeserializeVariables(version string) {
	// Open the local file that stores the variables' binary value
	b, err := ioutil.ReadFile(sw.Name + "-" + version)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Unmarshal the binary value
	var variables []interface{}
	json.Unmarshal(b, &variables)

	// Deserialize to get variables
	sw.variables = variables
}

// The channel to communicate with the supervisor
func (sw *SpoutWorker) TalkWithSupervisor() {
	// Message Type:
	// Superviosr -> Worker
	// 1. Please Serialize Variables With Version X    Superviosr -> Worker
	// 2. Please Kill Yourself                         Superviosr -> Worker
	// 3. Please Suspend                               Superviosr -> Worker
	// Worker -> Supervisor
	// 1. W Serialized Variables With Version X        Worker -> Supervisor
	// 2. W Suspended                                  Worker -> Supervisor

	for message := range sw.SupervisorC {
		switch string(message[0]) {
		case "1":
			words := strings.Fields(message)
			version := words[len(words) - 1]
			sw.SerializeVariables(version)
			fmt.Printf("Serialized Variables With Version %s\n", version)
			// Notify the supervisor it serialized the variables
			sw.WorkerC <- fmt.Sprintf("1. %s Serialized Variables With Version %s", sw.Name, version)

		case "2":
			sw.wg.Done()

		case "3":
			sw.suspend = true
			fmt.Printf("Suspended Spout Worker\n")
			sw.WorkerC <- fmt.Sprintf("2. %s Suspended", sw.Name)
		}
	}
}


