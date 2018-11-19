package bolt

import (
	"fmt"
)

type SpoutOutputCollector struct {
}

type OutputFieldsDeclarer struct {
}

type Spout interface {
	Open(conf map[string]string, collector SpoutOutputCollector)
	NextTuple()
	Close()
	DelareOutputFields(declarer OutputFieldsDeclarer)
}
