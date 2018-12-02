package main 

import (
	// "fmt"
	"time"
	"errors"
	"log"
	"os"
	"io/ioutil"
	"encoding/json"
	"strconv"
)


// Sample join bolt. (id, gender) + (id, age) -> (id, gender, age)
func GenderAgeJoinBolt(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Define variables
	var idMap map[string]interface{}
	// Initialize variables
	if (len(*variables) == 0) {
		idMap = make(map[string]interface{})
		for id_ := 1; id_ <= 5000; id_++ {
			idMap[strconv.Itoa(id_)] = make([]interface{}, 2)
		}
		*variables = append(*variables, idMap)
	}
	// Get variables
	idMap = (*variables)[0].(map[string]interface{})

	// Process logic
	id := tuple[0].(string)
	// _, ok := idMap[id]
	// if !ok {
	// 	idMap[id] = make([]interface{}, 2) // Create an interface array to store sex and age
	// }
	item := tuple[1].(string)
	if (item == "male" || item == "female") {
		idMap[id].([]interface{})[0] = item
		if idMap[id].([]interface{})[1] != nil {
			*result = []interface{}{id, idMap[id].([]interface{})[0], idMap[id].([]interface{})[1]}
		}
	} else {
		idMap[id].([]interface{})[1] = item
		if idMap[id].([]interface{})[0] != nil {
			*result = []interface{}{id, idMap[id].([]interface{})[0], idMap[id].([]interface{})[1]}
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
	var idArray interface{}
	var genderArray interface{}

	if (len(*variables) == 0) {
		counterMap = make(map[string]interface{})
		*variables = append(*variables, counterMap)
		counterMap["counter"] = float64(0)

		file, _ := os.Open("data.json")
		defer file.Close()

		b, _ := ioutil.ReadAll(file)
		var object interface{}
		json.Unmarshal(b, &object)
		objectMap := object.(map[string]interface{})
		idArray = objectMap["id"].([]interface{})
		genderArray = objectMap["gender"].([]interface{})

		*variables = append(*variables, idArray)
		*variables = append(*variables, genderArray)
	}
	counterMap = (*variables)[0].(map[string]interface{})
	idArray = (*variables)[1]
	genderArray = (*variables)[2]

	// Logic
	if counterMap["counter"].(float64) < 5000 {
		*result = []interface{}{idArray.([]interface{})[int(counterMap["counter"].(float64))], genderArray.([]interface{})[int(counterMap["counter"].(float64))]}
		counterMap["counter"] = counterMap["counter"].(float64) + 1
	}

	// // Logic
	// if counterMap["counter"].(float64) < 10 {
	// 	if int(counterMap["counter"].(float64)) % 2 == 0 {
	// 		*result = []interface{}{strconv.Itoa(int(counterMap["counter"].(float64))), "male"}
	// 	} else {
	// 		*result = []interface{}{strconv.Itoa(int(counterMap["counter"].(float64))), "female"}
	// 	}
	// 	counterMap["counter"] = counterMap["counter"].(float64) + 1
	// }

	time.Sleep(1 * time.Millisecond)

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
	var idArray interface{}
	var ageArray interface{}

	if (len(*variables) == 0) {
		counterMap = make(map[string]interface{})
		*variables = append(*variables, counterMap)
		counterMap["counter"] = float64(0)

		file, _ := os.Open("data.json")
		defer file.Close()

		b, _ := ioutil.ReadAll(file)
		var object interface{}
		json.Unmarshal(b, &object)
		objectMap := object.(map[string]interface{})
		idArray = objectMap["id"].([]interface{})
		ageArray = objectMap["age"].([]interface{})

		*variables = append(*variables, idArray)
		*variables = append(*variables, ageArray)
	}
	counterMap = (*variables)[0].(map[string]interface{})
	idArray = (*variables)[1]
	ageArray = (*variables)[2]

	// Logic
	if counterMap["counter"].(float64) < 5000 {
		*result = []interface{}{idArray.([]interface{})[int(counterMap["counter"].(float64))], ageArray.([]interface{})[int(counterMap["counter"].(float64))]}
		counterMap["counter"] = counterMap["counter"].(float64) + 1
	}

	// // Logic
	// if counterMap["counter"].(float64) < 800 {
	// 	*result = []interface{}{strconv.Itoa(int(counterMap["counter"].(float64))), strconv.Itoa(int(counterMap["counter"].(float64)) + 20)}
	// 	counterMap["counter"] = counterMap["counter"].(float64) + 1
	// }

	time.Sleep(1 * time.Millisecond)

	// Return value
	if (len(*result) > 0) {
		log.Printf("Age Spout Emit (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}