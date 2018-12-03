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
	sp := spout.NewSpoutInst("GenderSpout", "process.so", "GenderSpout", utils.GROUPING_BY_FIELD, 0)
	sp_ := spout.NewSpoutInst("AgeSpout", "process.so", "AgeSpout", utils.GROUPING_BY_FIELD, 0)
	sp.SetInstanceNum(1)
	sp_.SetInstanceNum(1)
	tm.AddSpout(sp)
	tm.AddSpout(sp_)

	// Create a bolt
	// Params: name, pluginFile, pluginSymbol, groupingHint, fieldIndex
	bm := bolt.NewBoltInst("GenderAgeJoinBolt", "process.so", "GenderAgeJoinBolt", utils.GROUPING_BY_ALL, 0)
	bm.SetInstanceNum(20)
	bm.AddPrevTaskName("GenderSpout")
	bm.AddPrevTaskName("AgeSpout")
	tm.AddBolt(bm)

	// Merge bolt
	mergeBolt := bolt.NewBoltInst("MergeBolt", "process.so", "MergeBolt", utils.GROUPING_BY_ALL, 0)
	mergeBolt.SetInstanceNum(1)
	mergeBolt.AddPrevTaskName("GenderAgeJoinBolt")
	tm.AddBolt(mergeBolt)

	tm.SubmitFile("./process.so", "process.so")
	// tm.SubmitFile("./data.json", "data.json")
	tm.Submit(":5050")
}