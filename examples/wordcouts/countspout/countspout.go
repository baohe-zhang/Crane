package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

type CountSpout struct {
	reader *bufio.Reader
}

func (cs *CountSpout) Open() {
	file, err := os.Open("twitter_combined.txt") // For read access.
	if err != nil {
		log.Fatal(err)
	}
	cs.reader = bufio.NewReader(file)
}

func (cs *CountSpout) NextTuple() []interface{} {
	b, _, err := cs.reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	line := strings.Split(string(b), " ")
	tuple := make([]interface{}, 0)
	tuple = append(tuple, line[0])
	tuple = append(tuple, line[1])
	return tuple
}

var CountSpouter CountSpout
