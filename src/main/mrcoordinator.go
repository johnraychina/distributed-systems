package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "distributed-systems/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	coordinator := mr.MakeCoordinator(os.Args[1:], 3, 10)
	for !mr.Done(coordinator) {
		time.Sleep(time.Second)
	}
	fmt.Println("coordinator detected no task, exiting...")

	time.Sleep(time.Second)
}
