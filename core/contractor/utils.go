package main 

import (
	"hash/fnv"
	"encoding/json"
)

func hash(value interface{}) int {
	bytes, _ := json.Marshal(value)
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}