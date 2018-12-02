package main 

import (
	// "fmt"
	"time"
	"errors"
	"log"
	// "os"
	// "bufio"
	"strconv"
)


// Sample join bolt. (id, gender) + (id, age) -> (id, gender, age)
func GenderAgeJoinBolt(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Define variables
	var idMap map[string][]interface{}
	// Initialize variables
	if (len(*variables) == 0) {
		idMap = make(map[string][]interface{})
		*variables = append(*variables, idMap)
	}
	// Get variables
	idMap = (*variables)[0].(map[string][]interface{})

	// Process logic
	id := tuple[0].(string)
	_, ok := idMap[id]
	if !ok {
		idMap[id] = make([]interface{}, 2) // Create an interface array to store sex and age
	}
	item := tuple[1].(string)
	if (item == "male" || item == "female") {
		idMap[id][0] = item
		if idMap[id][1] != nil {
			*result = []interface{}{id, idMap[id][0], idMap[id][1]}
		}
	} else {
		idMap[id][1] = item
		if idMap[id][0] != nil {
			*result = []interface{}{id, idMap[id][0], idMap[id][1]}
		}
	}
	if len(*result) > 0 {
		log.Printf("Join Bolt Emit (%v)\n", *result)
	}
	return nil
}

// Sample gender spout. emit (id, gender)
func GenderSpout(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Variables
	var counterMap map[string]interface{}
	if (len(*variables) == 0) {
		counterMap = make(map[string]interface{})
		*variables = append(*variables, counterMap)
		counterMap["counter"] = float64(0)
	}
	counterMap = (*variables)[0].(map[string]interface{})

	// Logic
	if counterMap["counter"].(float64) < 800 {
		if int(counterMap["counter"].(float64)) % 2 == 0 {
			*result = []interface{}{strconv.Itoa(int(counterMap["counter"].(float64))), "male"}
		} else {
			*result = []interface{}{strconv.Itoa(int(counterMap["counter"].(float64))), "female"}
		}
		counterMap["counter"] = counterMap["counter"].(float64) + 1
	}

	time.Sleep(100 * time.Millisecond)

	// Return value
	if (len(*result) > 0) {
		log.Printf("Gender Spout Emit (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}

// Sample age spout. emit (id, age)
func AgeSpout(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Variables
	var counterMap map[string]interface{}
	if (len(*variables) == 0) {
		counterMap = make(map[string]interface{})
		*variables = append(*variables, counterMap)
		counterMap["counter"] = float64(0)
	}
	counterMap = (*variables)[0].(map[string]interface{})

	// Logic
	if counterMap["counter"].(float64) < 800 {
		*result = []interface{}{strconv.Itoa(int(counterMap["counter"].(float64))), strconv.Itoa(int(counterMap["counter"].(float64)) + 20)}
		counterMap["counter"] = counterMap["counter"].(float64) + 1
	}

	time.Sleep(100 * time.Millisecond)

	// Return value
	if (len(*result) > 0) {
		log.Printf("Age Spout Emit (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}