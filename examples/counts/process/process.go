package main 

import (
	// "fmt"
	"log"
	"time"
	"errors"
	"strings"
	"os"
	"bufio"
)

// Sample word split bolt
func WordSplitBolt(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Process Logic
	sentence := tuple[0].(string)
	words := strings.Fields(sentence)

	// for _, word := range words {
	// 	*result = append(*result, word)
	// }
	*result = []interface{}{words[0]}

	if len(*result) > 0 {
		log.Printf("Word Split Bolt Emit: (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}

// Sample word count bolt
func WordCountBolt(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Bolt's state variables
	var countMap map[string]interface{}
	if (len(*variables) == 0) {
		// Initialize variables
		countMap = make(map[string]interface{})
		*variables = append(*variables, countMap)
	}
	countMap = (*variables)[0].(map[string]interface{})

	// Bolt's process logic
	for index, _ := range tuple {
		word := tuple[index].(string)
		_, ok := countMap[word]
		if !ok {
			countMap[word] = float64(0)
		}
		countMap[word] = countMap[word].(float64) + 1
		log.Printf("Word Count Bolt Emit: (%v)\n", []interface{}{word, countMap[word].(float64)})
	}

	// Return value
	if (len(*result) > 0) {
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}

// Sample word generator
func WordSpout(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	// Variables
	var counterMap map[string]interface{}

	if (len(*variables) == 0) {
		// Initialize variables
		counterMap = make(map[string]interface{})
		*variables = append(*variables, counterMap)
		counterMap["counter"] = float64(0)

		*variables = append(*variables, make([]string, 0))

		file, _ := os.Open("corputs.txt")
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			(*variables)[1] = append((*variables)[1].([]string), scanner.Text())
		}
	}
	counterMap = (*variables)[0].(map[string]interface{})

	// Logic
	if counterMap["counter"].(float64) < 2000 {
		log.Printf("Sentence Spout Counter %v\n", counterMap["counter"])
		*result = []interface{}{(*variables)[1].([]string)[int(counterMap["counter"].(float64)) % len((*variables)[1].([]string))]}
		log.Printf("Sentence Spout Emit: (%v)\n", *result)
		counterMap["counter"] = counterMap["counter"].(float64) + 1
	}
	time.Sleep(1 * time.Millisecond)

	// Return value
	if (len(*result) > 0) {
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}



// func ProcFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	num := tuple[0].(float64)
// 	num *= 2
// 	fmt.Println("bolt emit: ", num)
// 	*result = []interface{}{num}

// 	return nil
// }

// // Sample counter
// func NextTuple(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	var counter *float64
// 	if (len(*variables) == 0) {
// 		counter = new(float64)
// 		*variables = append(*variables, counter)
// 	}
// 	counter = ((*variables)[0]).(*float64)

// 	if *counter < 101 {
// 		fmt.Println("spout emit: ", *counter)
// 		*result = []interface{}{*counter}
// 		(*counter)++
// 	}

// 	time.Sleep(100 * time.Millisecond)

// 	if (len(*result) > 0) {
// 		return nil
// 	} else {
// 		return errors.New("next tuple is nil")
// 	}
// }

// // Sample File Reader
// func NextTuple(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	// Create or get state variables
// 	var reader *bufio.Reader
// 	if (len(*variables) == 0) {
// 		file, err := os.Open("/Users/zhangbaohe/go/src/crane/core/process/integers.txt")
// 		if err != nil {
// 			fmt.Println(err)
// 			os.Exit(1)
// 		}
// 		reader = bufio.NewReader(file)
// 		*variables = append(*variables, reader)
// 	}
// 	reader = ((*variables)[0]).(*bufio.Reader)

// 	// Generate next tuple
// 	b, err := reader.ReadByte()
// 	if err != nil {
// 		return errors.New("next tuple is nil")
// 	}

// 	*result = []interface{}{float64(b)}

// 	time.Sleep(100 * time.Millisecond)

// 	return nil
// }


// // Sample Multiply Two
// func processorFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	num := tuple[0].(float64)
// 	num *= 2
// 	*result = []interface{}{num}

// 	return nil
// }