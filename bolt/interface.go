package bolt

import ()

type BoltInst struct {
	Name          string
	PrevTaskNames []string
	TaskAddrs     []string
	PluginFile    string
	PluginSymbol  string
	GroupingHint  string
	FieldIndex    int
	InstNum       int
}

func NewBoltInst(name, pluginFile, pluginSymbol, grouping string, mainField int) *BoltInst {
	boltInst := &BoltInst{}
	boltInst.Name = name
	boltInst.PluginFile = pluginFile
	boltInst.PluginSymbol = pluginSymbol
	boltInst.GroupingHint = grouping
	boltInst.FieldIndex = mainField
	boltInst.InstNum = 1
	boltInst.PrevTaskNames = make([]string, 0)
	return boltInst
}

func (bi *BoltInst) SetInstanceNum(n int) {
	bi.InstNum = n
}

func (bi *BoltInst) AddPrevTaskName(task string) {
	bi.PrevTaskNames = append(bi.PrevTaskNames, task)
}

type BoltOutputCollector struct {
}

type Bolt interface {
	Execute()
}
