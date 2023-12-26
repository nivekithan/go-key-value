package main

import (
	"flag"
	"fmt"
	goraft "kv/internal/goraft"
	"time"
)

func main() {
	id := flag.Uint64("id", 0, "Id of the node")
	totalNodes := flag.Uint64("total_nodes", 0, "No of nodes in the cluster")

	flag.Parse()

	address := fmt.Sprintf(":%d", 9000+*id)

	allMembers := []goraft.Member{}

	for i := uint64(1); i <= *totalNodes; i++ {
		if i == *id {
			continue
		}

		member := goraft.Member{Address: fmt.Sprintf(":%d", 9000+i)}

		allMembers = append(allMembers, member)
	}

	raftServer := goraft.NewRaftServer(goraft.Config{
		Id:          *id,
		Address:     address,
		HeartBeatMs: 2000,
		Members:     allMembers,
	})

	raftServer.Start()

	ticker := time.NewTicker(time.Duration(10) * time.Second)

	for range ticker.C {
		raftServer.AddEntry(fmt.Sprintf("Command for node %d", *id))
		ticker.Stop()
	}

	ch := make(chan bool)

	for range ch {

	}
}
