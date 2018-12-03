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

	// Create a Integer Spout
	sp := spout.NewSpoutInst("IntegerSpout", "process.so", "IntegerSpout", utils.GROUPING_BY_SHUFFLE, 0)
	sp.SetInstanceNum(1)
	tm.AddSpout(sp)

	// Multiply Bolt
	// Params: name, pluginFile, pluginSymbol, groupingHint, fieldIndex
	mb := bolt.NewBoltInst("MultiplyBolt", "process.so", "MultiplyBolt", utils.GROUPING_BY_SHUFFLE, 0)
	mb.SetInstanceNum(4)
	mb.AddPrevTaskName("IntegerSpout")
	tm.AddBolt(mb)

	// Divide Bolt
	db := bolt.NewBoltInst("DivideBolt", "process.so", "DivideBolt", utils.GROUPING_BY_ALL, 0)
	db.SetInstanceNum(4)
	db.AddPrevTaskName("MultiplyBolt")
	tm.AddBolt(db)

	tm.SubmitFile("./process.so", "process.so")
	// tm.SubmitFile("./data.json", "data.json")
	tm.Submit(":5050")
}
