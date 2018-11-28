package boltworker 

import (
	"hash/fnv"
	"encoding/json"
	"plugin"
	"os"
	"fmt"
)

func hash(value interface{}) int {
	bytes, _ := json.Marshal(value)
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

func lookupProcFunc(procFuncName string) func([]interface{}, *[]interface{}, *[]interface{}) error {
	// Load module
	plug, err := plugin.Open("../process/process.so")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Look up a symbol, in this case, the ProcFunc
	symProcFunc, err := plug.Lookup(procFuncName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Assert the symbol is the desired one
	var procFunc func([]interface{}, *[]interface{}, *[]interface{}) error
	procFunc, ok := symProcFunc.(func([]interface{}, *[]interface{}, *[]interface{}) error)
	if !ok {
		fmt.Println("unexpected type from module symbol")
		os.Exit(1)
	}

	return procFunc
}