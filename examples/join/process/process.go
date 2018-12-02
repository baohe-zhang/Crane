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
	log.Printf("Bolt Emit (%v)\n", *result)
	return nil
}

// Sample gender spout. emit (id, gender)
func GenderSpout(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Variables
	var counter interface{}
	if (len(*variables) == 0) {
		*variables = append(*variables, counter)
		counter = float64(1)
	}
	counter = (*variables)[0]

	// Logic
	if counter.(float64) < 21 {
		if int(counter.(float64)) % 2 == 0 {
			*result = []interface{}{strconv.Itoa(int(counter.(float64))), "male"}
		} else {
			*result = []interface{}{strconv.Itoa(int(counter.(float64))), "female"}
		}
		counter = counter.(float64) + 1
	}

	time.Sleep(100 * time.Millisecond)

	// Return value
	if (len(*result) > 0) {
		log.Printf("Spout Emit (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}



// // Sample word count bolt
// func ProcFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	// Bolt's global variables
// 	var countMap map[string]interface{}
// 	if (len(*variables) == 0) {
// 		// Initialize variables
// 		countMap = make(map[string]interface{})
// 		*variables = append(*variables, countMap)
// 	}
// 	countMap = (*variables)[0].(map[string]interface{})

// 	// Bolt's process logic
// 	word := tuple[0].(string)
// 	_, ok := countMap[word]
// 	if !ok {
// 		countMap[word] = float64(0)
// 	}
// 	countMap[word] = countMap[word].(float64) + 1
// 	*result = []interface{}{word, countMap[word].(float64)}
// 	fmt.Printf("bolt emit: (%v)\n", *result)

// 	return nil
// }

// // Sample word generator
// func NextTuple(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	// Variables
// 	words := []string{"china", "usa", "japan", "korea", "russia", "india", "singapore"}
// 	var counterMap map[string]interface{}

// 	if (len(*variables) == 0) {
// 		// Initialize variables
// 		counterMap = make(map[string]interface{})
// 		*variables = append(*variables, counterMap)
// 		counterMap["counter"] = float64(0)
// 	}
// 	counterMap = (*variables)[0].(map[string]interface{})

// 	// Logic
// 	if counterMap["counter"].(float64) < 800 {
// 		fmt.Printf("spout counter %v\n", counterMap["counter"])
// 		*result = []interface{}{words[int(counterMap["counter"].(float64)) % len(words)]}
// 		fmt.Printf("spout emit: (%v)\n", *result)
// 		counterMap["counter"] = counterMap["counter"].(float64) + 1
// 	}
// 	time.Sleep(100 * time.Millisecond)

// 	// Return value
// 	if (len(*result) > 0) {
// 		return nil
// 	} else {
// 		return errors.New("next tuple is nil")
// 	}
// }