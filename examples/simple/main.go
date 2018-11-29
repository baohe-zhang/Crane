package main

import (
	"crane/bolt"
	"crane/core/utils"
	"crane/spout"
	"crane/topology"
)

func main() {
	// Create a topology
	tm := topology.Topology{}

	// Create a spout
	sp := spout.NewSpoutInst("NextTuple", "process.so", "NextTuple", utils.GROUPING_BY_FIELD, 0)
	sp.SetInstanceNum(1)
	tm.AddSpout(sp)

	// Create a bolt
	// Params: name, pluginFile, pluginSymbol, groupingHint, fieldIndex
	bm := bolt.NewBoltInst("ProcFunc", "process.so", "ProcFunc", utils.GROUPING_BY_ALL, 0)
	bm.SetInstanceNum(4)
	bm.AddPrevTaskName("NextTuple")
	tm.AddBolt(bm)

	tm.SubmitFile("../process/process.so", "process.so")
	tm.Submit("fa18-cs425-g29-01.cs.illinois.edu:5050")
}
