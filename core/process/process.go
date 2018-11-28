package main 

import (
	"fmt"
	"time"
	"errors"
	// "os"
	// "bufio"
)

func ProcFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	num := tuple[0].(float64)
	num *= 2
	fmt.Println("bolt emit: ", num)
	*result = []interface{}{num}

	return nil
}

// Sample counter
func NextTuple(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	var counter *float64
	if (len(*variables) == 0) {
		counter = new(float64)
		*variables = append(*variables, counter)
	}
	counter = ((*variables)[0]).(*float64)

	if *counter < 101 {
		fmt.Println("spout emit: ", *counter)
		*result = []interface{}{*counter}
		(*counter)++
	}

	time.Sleep(100 * time.Millisecond)

	if (len(*result) > 0) {
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}

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


// // Sample Word Count
// func processorFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	// Bolt's global variables
// 	var countMap map[string]int
// 	if (len(*variables) == 0) {
// 		// Initialize variables
// 		countMap = make(map[string]int)
// 		*variables = append(*variables, countMap)
// 	} else {
// 		countMap = (*variables)[0].(map[string]int)
// 	}

// 	// Bolt's process logic
// 	word := tuple[0].(string)
// 	count, ok := countMap[word]
// 	if !ok {
// 		count = 0
// 	}
// 	count++
// 	countMap[word] = count
// 	*result = []interface{}{word, count}

// 	return nil
// }


// // Sample Multiply Two
// func processorFunc(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
// 	num := tuple[0].(float64)
// 	num *= 2
// 	*result = []interface{}{num}

// 	return nil
// }