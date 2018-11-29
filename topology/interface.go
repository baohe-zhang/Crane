package topology

import (
	"crane/bolt"
	"crane/core/client"
	"crane/core/utils"
	"crane/spout"
	"fmt"
	"log"
	"os/exec"
	"os/user"
)

// Topology interface for bolts and spouts submissions to driver
type Topology struct {
	Bolts  []bolt.BoltInst
	Spouts []spout.SpoutInst
}

// Factory mode to create a new Topology instance
func NewTopology() *Topology {
	topology := &Topology{}
	topology.Bolts = make([]bolt.BoltInst, 0)
	topology.Spouts = make([]spout.SpoutInst, 0)
	return topology
}

// Add a new spout instance
func (t *Topology) AddSpout(s *spout.SpoutInst) {
	t.Spouts = append(t.Spouts, *s)
}

// Add a new bolt instance
func (t *Topology) AddBolt(b *bolt.BoltInst) {
	t.Bolts = append(t.Bolts, *b)
}

// Submit the topology
func (t *Topology) Submit(driverAddr string) {
	client := client.NewClient(driverAddr)
	if client == nil {
		log.Println("Initialize client failed")
		return
	}
	b, err := utils.Marshal(utils.TOPO_SUBMISSION, *t)
	if err != nil {
		log.Println(err)
		return
	}
	client.ContactDriver(b)
	client.Start()
}

// Submit the related file to distributed file system
func (t *Topology) SubmitFile(localPath, remoteName string) {
	usr, _ := user.Current()
	usrHome := usr.HomeDir
	cmd := exec.Command(usrHome+"go/src/crane/tools/sdfs_client/sdfs_client", "-master", "fa18-cs425-g29-01.cs.illinois.edu:5000", "put", localPath, remoteName)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", stdoutStderr)
}
