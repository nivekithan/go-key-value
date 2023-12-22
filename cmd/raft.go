package main

import (
	"flag"
	"fmt"
	goraft "kv"
	"os"
)

func main() {
	tempFile, err := os.CreateTemp("", "raft-main")

	if err != nil {
		panic(err)
	}

	defer tempFile.Close()

	id := flag.Int("id", 0, "Id of the node")
	totalNodes := flag.Int("total_nodes", 0, "No of nodes in the cluster")

	flag.Parse()

	address := fmt.Sprintf(":%d", 9000+*id)

	clusterMember := []goraft.Member{}
	for i := 1; i <= *totalNodes; i++ {
		if i == *id {
			continue
		}

		member := goraft.Member{Id: uint64(i), Address: fmt.Sprintf(":%d", 9000+i)}

		clusterMember = append(clusterMember, member)
	}

	raftServer := goraft.NewRaftServer(tempFile, goraft.Config{
		HeartbeatMs: 5000,
		Member:      clusterMember,
		Address:     address,
		Id:          uint64(*id),
	})

	raftServer.Start()
	ch := make(chan bool)

	for _ = range ch {

	}
}
