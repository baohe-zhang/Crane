package main

import (
	"crane/spout"
	"fmt"
	"os"
	"plugin"
)

func main() {
	// load module
	plug, err := plugin.Open("countspout.so")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// look up a symbol (an exported function or variable)
	// in this case, variable Greeter
	symGreet, err := plug.Lookup("CountSpouter")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Assert that loaded symbol is of a desired type
	// in this case interface type Greeter (defined above)
	cs, ok := symGreet.(spout.Spout)
	if !ok {
		fmt.Println("unexpected type from module symbol")
		os.Exit(1)
	}
	cs.Open()
	fmt.Println(cs.NextTuple())
}
