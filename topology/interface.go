package topology

import (
	"crane/bolt"
	"crane/spout"
	//"log"
)

type Topology struct {
	Bolts  []bolt.BoltInst
	Spouts []spout.SpoutInst
}

func NewTopology() *Topology {
	topology := &Topology{}
	topology.Bolts = make([]bolt.BoltInst, 0)
	topology.Spouts = make([]spout.SpoutInst, 0)
	return topology
}

func (t *Topology) AddSpout(s *spout.SpoutInst) {
	t.Spouts = append(t.Spouts, *s)
}

func (t *Topology) AddBolt(b *bolt.BoltInst) {
	t.Bolts = append(t.Bolts, *b)
}

func (t *Topology) Submit(driverAddr string) {

}
