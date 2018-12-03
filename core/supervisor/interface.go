package main

import (
	"crane/core/boltworker"
	"crane/core/messages"
	"crane/core/spoutworker"
	"crane/core/utils"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Supervisor, the slave node for accepting the schedule from the master node
// and execute the task, spouts or bolts
type Supervisor struct {
	Sub                      *messages.Subscriber
	BoltWorkers              []*boltworker.BoltWorker
	SpoutWorkers             []*spoutworker.SpoutWorker
	VmIndexMap               map[int]string
	FilePathMap              map[string]string
	SerializeResponseCounter int
	Mutex                    sync.Mutex
	ControlC                 chan string
}

// Factory mode to return the Supervisor instance
func NewSupervisor(driverAddr string) *Supervisor {
	supervisor := &Supervisor{}
	supervisor.Sub = messages.NewSubscriber(driverAddr)
	if supervisor.Sub == nil {
		return nil
	}
	supervisor.BoltWorkers = make([]*boltworker.BoltWorker, 0)
	supervisor.SpoutWorkers = make([]*spoutworker.SpoutWorker, 0)
	supervisor.VmIndexMap = make(map[int]string)
	supervisor.FilePathMap = make(map[string]string)
	supervisor.SerializeResponseCounter = 0
	supervisor.ControlC = make(chan string)
	return supervisor
}

// Daemon function for supervisor service
func (s *Supervisor) StartDaemon() {
	go s.Sub.RequestMessage()
	go s.Sub.ReadMessage()
	s.SendJoinRequest()

	for {
		select {
		case rcvMsg := <-s.Sub.PublishBoard:
			payload := utils.CheckType(rcvMsg.Payload)

			switch payload.Header.Type {
			case utils.FILE_PULL:
				filePull := &utils.FilePull{}
				utils.Unmarshal(payload.Content, filePull)
				log.Printf("Receive File Pull with Filename %s\n", filePull.Filename)
				if filePull.Filename != "None" {
					s.GetFile(filePull.Filename)
				}

			case utils.BOLT_TASK:
				task := &utils.BoltTaskMessage{}
				utils.Unmarshal(payload.Content, task)
				log.Printf("Receive Bolt Dispatch %s with Port %s, Previous workers %v\n", task.Name, task.Port, task.PrevBoltAddr)
				supervisorC := make(chan string) // Channel to talk to the worker
				workerC := make(chan string)     // Channel to listen to the worker
				bw := boltworker.NewBoltWorker(1, task.Name, "./"+task.PluginFile, task.PluginSymbol,
					task.Port, task.PrevBoltAddr, task.PrevBoltGroupingHint, task.PrevBoltFieldIndex,
					task.SuccBoltGroupingHint, task.SuccBoltFieldIndex, supervisorC, workerC, task.SnapshotVersion)
				s.BoltWorkers = append(s.BoltWorkers, bw)

			case utils.SPOUT_TASK:
				task := &utils.SpoutTaskMessage{}
				utils.Unmarshal(payload.Content, task)
				log.Printf("Receive Spout Dispatch %s with Port %s\n", task.Name, task.Port)
				supervisorC := make(chan string)
				workerC := make(chan string)
				sw := spoutworker.NewSpoutWorker(task.Name, "./"+task.PluginFile, task.PluginSymbol, task.Port,
					task.GroupingHint, task.FieldIndex, supervisorC, workerC, task.SnapshotVersion)
				s.SpoutWorkers = append(s.SpoutWorkers, sw)

			case utils.TASK_ALL_DISPATCHED:
				log.Printf("Receive Task All Dispatched, Worker Start...\n")
				for _, sw := range s.SpoutWorkers {
					go sw.Start()
				}
				for _, bw := range s.BoltWorkers {
					go bw.Start()
				}
				time.Sleep(100 * time.Millisecond)
				go s.ListenToWorkers()

			case utils.SUSPEND_REQUEST:
				log.Printf("Receive Suspend Request\n")
				s.SendSuspendRequestToWorkers()

			case utils.SNAPSHOT_REQUEST:
				var version int
				utils.Unmarshal(payload.Content, &version)
				log.Printf("Receive Snapshot Request With Version %d\n", version)
				s.SendSerializeRequestToWorkers(strconv.Itoa(version))

			case utils.RESTORE_REQUEST:
				s.ControlC <- "Close"
				log.Printf("Receive Restore Request")
				// Clear supervisor's worker map
				s.BoltWorkers = make([]*boltworker.BoltWorker, 0)
				s.SpoutWorkers = make([]*spoutworker.SpoutWorker, 0)
			}
			/*default:*/
			/*time.Sleep(10 * time.Millisecond)*/
		}
	}

}

// Send join request to join the cluster
func (s *Supervisor) SendJoinRequest() {
	log.Printf("Send Join Request")
	join := utils.JoinRequest{Name: "vm [" + s.Sub.Conn.LocalAddr().String() + "]"}
	b, err := utils.Marshal(utils.JOIN_REQUEST, join)
	if err != nil {
		log.Println(err)
		return
	}
	s.Sub.Request <- messages.Message{
		Payload:      b,
		TargetConnId: s.Sub.Conn.RemoteAddr().String(),
	}
}

// Listen workers reply through channels
// Should be closed when receive restore request
func (s *Supervisor) ListenToWorkers() {
	// Message Type:
	// Superviosr -> Worker
	// 1. Please Serialize Variables With Version X    Superviosr -> Worker
	// 2. Please Kill Yourself                         Superviosr -> Worker
	// 3. Please Suspend                               Superviosr -> Worker
	// Worker -> Supervisor
	// 1. Serialized Variables With Version X          Worker -> Supervisor
	// 2. W Suspended                                  Worker -> Supervisor

	log.Println("Start Listen To Workers")

	for {
		select {
		// Channel to close this goroutine
		case signal := <-s.ControlC:
			s.SendKillRequestToWorkers()
			time.Sleep(120 * time.Millisecond)
			log.Printf("Receive Signal %s, Listen To Workers Return\n", signal)
			return
		default:
		}

		for _, bw := range s.BoltWorkers {
			select {
			case message := <-bw.WorkerC:
				switch string(message[0]) {
				case "1":
					s.PutFile("./"+bw.Name+"_"+bw.Version, bw.Name+"_"+bw.Version)
					s.SerializeResponseCounter += 1
					if s.SerializeResponseCounter == (len(s.BoltWorkers) + len(s.SpoutWorkers)) {
						s.SerializeResponseCounter = 0
						s.SendSerializeResponseToDriver()
						s.SendResumeRequestToWorkers()
					}
				}
			default:
			}
		}

		for _, sw := range s.SpoutWorkers {
			select {
			case message := <-sw.WorkerC:
				switch string(message[0]) {
				case "1":
					go s.PutFile("./"+sw.Name+"_"+sw.Version, sw.Name+"_"+sw.Version)
					s.SerializeResponseCounter += 1
					if s.SerializeResponseCounter == (len(s.BoltWorkers) + len(s.SpoutWorkers)) {
						s.SerializeResponseCounter = 0
						s.SendResumeRequestToWorkers()
						s.SendSerializeResponseToDriver()
					}
				case "2":
					s.SendSuspendResponseToDriver()
				}
			default:
			}
		}

	}

	// var wg sync.WaitGroup
	// wg.Add(1)
	// wg.Wait()
}

// Notify the driver that the spout is suspended
func (s *Supervisor) SendSuspendResponseToDriver() {
	log.Println("Send Suspend Reponse To Driver")
	b, _ := utils.Marshal(utils.SUSPEND_RESPONSE, "OK")
	s.Sub.Request <- messages.Message{
		Payload:      b,
		TargetConnId: s.Sub.Conn.RemoteAddr().String(),
	}
}

// Notify the driver that the serialize is finished
func (s *Supervisor) SendSerializeResponseToDriver() {
	log.Println("Send Serialize Reponse To Driver")
	b, _ := utils.Marshal(utils.SNAPSHOT_RESPONSE, "OK")
	s.Sub.Request <- messages.Message{
		Payload:      b,
		TargetConnId: s.Sub.Conn.RemoteAddr().String(),
	}
}

// Notify all workers to serialize their variables
func (s *Supervisor) SendSerializeRequestToWorkers(version string) {
	// Message Type:
	// Superviosr -> Worker
	// 1. Please Serialize Variables With Version X    Superviosr -> Worker
	// 2. Please Kill Yourself                         Superviosr -> Worker
	// 3. Please Suspend                               Superviosr -> Spout Worker
	// 4. Please Resume                                Superviosr -> Spout Worker
	// Worker -> Supervisor
	// 1. Serialized Variables With Version X          Worker -> Supervisor
	// 2. W Suspended                                  Worker -> Supervisor

	log.Println("Send Serialize Request to Workers")
	for _, bw := range s.BoltWorkers {
		bw.SupervisorC <- fmt.Sprintf("1. Please Serialize Variables With Version %s", version)
	}
	for _, sw := range s.SpoutWorkers {
		sw.SupervisorC <- fmt.Sprintf("1. Please Serialize Variables With Version %s", version)
	}
}

// Notify all workers to kill themselves
func (s *Supervisor) SendKillRequestToWorkers() {
	log.Println("Send Kill Request to Workers")
	for _, bw := range s.BoltWorkers {
		bw.SupervisorC <- fmt.Sprintf("2. Please Kill Yourself")
	}
	for _, sw := range s.SpoutWorkers {
		sw.SupervisorC <- fmt.Sprintf("2. Please Kill Yourself")
	}
}

// Ask spout to suspend
func (s *Supervisor) SendSuspendRequestToWorkers() {
	log.Println("Send Suspend Request to Spout Workers")
	for _, sw := range s.SpoutWorkers {
		sw.SupervisorC <- fmt.Sprintf("3. Please Suspend")
	}
}

// Ask spout to resume
func (s *Supervisor) SendResumeRequestToWorkers() {
	log.Println("Send Resume Request to Spout Workers")
	for _, sw := range s.SpoutWorkers {
		sw.SupervisorC <- fmt.Sprintf("4. Please Resume")
	}
}

// Get the plugin file from distributed file system
func (s *Supervisor) GetFile(remoteName string) {
	time.Sleep(1 * time.Second)
	_, ok := s.FilePathMap[remoteName]
	if ok {
		return
	}
	// Execute the sdfs client to get the remote file
	usr, _ := user.Current()
	usrHome := usr.HomeDir
	// exec.Command(usrHome+"/go/src/crane/tools/sdfs_client/sdfs_client", "-master", "fa18-cs425-g29-01.cs.illinois.edu:5000", "get", remoteName, "./"+remoteName)
	cmd := exec.Command(usrHome+"/go/src/crane/tools/sdfs_client/sdfs_client", "-master", "fa18-cs425-g29-01.cs.illinois.edu:5000", "get", remoteName, "./"+remoteName)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s\n", stdoutStderr)
	s.FilePathMap[remoteName] = "./" + remoteName
	log.Printf("Get File %s", remoteName)
}

// Put State File into Distributed File System
func (s *Supervisor) PutFile(localPath, remoteName string) {
	time.Sleep(1 * time.Second)
	// Execute the sdfs client to put the local file into remote
	usr, _ := user.Current()
	usrHome := usr.HomeDir
	// exec.Command(usrHome+"/go/src/crane/tools/sdfs_client/sdfs_client", "-master", "fa18-cs425-g29-01.cs.illinois.edu:5000", "put", localPath, remoteName)
	cmd := exec.Command(usrHome+"/go/src/crane/tools/sdfs_client/sdfs_client", "-master", "fa18-cs425-g29-01.cs.illinois.edu:5000", "put", localPath, remoteName)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s\n", stdoutStderr)
	log.Printf("Put File %s", remoteName)
}

func main() {
	driverIpPtr := flag.String("h", "127.0.0.1", "Driver's IP address")
	vmIndexPtr := flag.Int("vm", 0, "VM index in cluster")
	flag.Parse()
	vms := utils.GetVmMap()
	var ip string
	if *vmIndexPtr <= 0 && *driverIpPtr == "127.0.0.1" {
		ip = *driverIpPtr
		log.Println("Enter Local Mode")
	} else if *vmIndexPtr != 0 {
		if *vmIndexPtr > 10 {
			log.Fatal("VM Cluster Index out of range")
			return
		} else {
			ip = vms[*vmIndexPtr]
			log.Println("Enter Cluster mode")
		}
	} else {
		ip = *driverIpPtr
		log.Println("Enter Remote Mode")
	}

	// remove all sdfs files set up before
	files, err := filepath.Glob("./*_*")
	if err != nil {
		log.Println(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			log.Println(err)
		}
	}

	LocalIP := utils.GetLocalIP().String()
	LocalHostname := utils.GetLocalHostname()
	log.Printf("Local Machine Info [%s] [%s]\n", LocalIP, LocalHostname)

	supervisor := NewSupervisor(ip + ":" + fmt.Sprintf("%d", utils.DRIVER_PORT))
	if supervisor == nil {
		log.Println("Initialize supervisor failed")
		return
	}
	supervisor.VmIndexMap = vms
	supervisor.StartDaemon()
}
