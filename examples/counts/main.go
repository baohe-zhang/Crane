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
	sp := spout.NewSpoutInst("WordSpout", "process.so", "WordSpout", utils.GROUPING_BY_FIELD, 0)
	sp.SetInstanceNum(1)
	tm.AddSpout(sp)

	// Create a bolt
	// Params: name, pluginFile, pluginSymbol, groupingHint, fieldIndex
	// sb := bolt.NewBoltInst("WordSplitBolt", "process.so", "WordSplitBolt", utils.GROUPING_BY_FIELD, 0)
	// sb.SetInstanceNum(1)
	// sb.AddPrevTaskName("WordSpout")
	// tm.AddBolt(sb)

	cb := bolt.NewBoltInst("WordCountBolt", "process.so", "WordCountBolt", utils.GROUPING_BY_ALL, 0)
	cb.SetInstanceNum(8)
	cb.AddPrevTaskName("WordSpout")
	tm.AddBolt(cb)

	tm.SubmitFile("./process.so", "process.so")
	tm.Submit(":5050")
}