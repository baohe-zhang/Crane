package main

type SpoutInst struct {
	Name         string
	InputFile    string
	PluginFile   string
	PluginSymbol string
	GroupingHint string
	FieldIndex   int
	InstNum      int
	TaskAddrs    []string
}

func NewSpoutInst(name, pluginFile, pluginSymbol string, grouping string, mainField int) *SpoutInst {
	spoutInst := &SpoutInst{}
	spoutInst.Name = name
	spoutInst.PluginFile = pluginFile
	spoutInst.PluginSymbol = pluginSymbol
	spoutInst.GroupingHint = grouping
	spoutInst.FieldIndex = mainField
	spoutInst.InstNum = 1
	spoutInst.TaskAddrs = make([]string, 0)
	return spoutInst
}

func (si *SpoutInst) SetInstanceNum(n int) {
	if n > 0 {
		si.InstNum = n
	}
}

func (si *SpoutInst) SetInputFile(input string) {
	si.InputFile = input
}
