package main 

import (
	// "fmt"
	"log"
	"time"
	"errors"
	// "strings"
	// "os"
	// "bufio"
)

// Sample Divide Two Bolt
func DivideBolt(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	num := tuple[0].(float64)
	num /= 2
	*result = []interface{}{num}

	// Return value
	if (len(*result) > 0) {
		log.Printf("Divide Bolt Emit (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}


// Sample Multiply Two Bolt
func MultiplyBolt(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	num := tuple[0].(float64)
	num *= 2
	*result = []interface{}{num}

	// Return value
	if (len(*result) > 0) {
		log.Printf("Multiply Bolt Emit (%v)\n", *result)
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}

// Integer generator
func IntegerSpout(tuple []interface{}, result *[]interface{}, variables *[]interface{}) error {
	var counter *float64
	if (len(*variables) == 0) {
		counter = new(float64)
		*variables = append(*variables, counter)
	}
	counter = ((*variables)[0]).(*float64)

	if *counter < 10000 {
		log.Printf("Integer Spout Emit (%d)\n", *counter)
		*result = []interface{}{*counter}
	}
	(*counter)++

	time.Sleep(1 * time.Millisecond)

	if (len(*result) > 0) {
		return nil
	} else {
		return errors.New("next tuple is nil")
	}
}