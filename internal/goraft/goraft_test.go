package goraft

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestLeader(t *testing.T) {
	heartbeatTimeout := 200
	raftCluster := makeRaftCluster(3, heartbeatTimeout)

	testTicker := time.NewTicker(time.Duration(heartbeatTimeout) * time.Millisecond)

	defer testTicker.Stop()

	timeout := time.NewTicker(time.Duration(heartbeatTimeout*10) * time.Millisecond)

	for {

		select {
		case <-testTicker.C:
			for _, raftServer := range raftCluster {
				if raftServer.memoryState.state() == leaderState {
					t.Logf("Leader server is %d", raftServer.memoryState.id())
					return
				}
			}
		case <-timeout.C:
			t.Fatal("No leader is formed in 10seconds")
		}
	}
}

func makeRaftCluster(noOfServers int, heartbeatTimeoutMs int) []*RaftServer {
	allRaftServer := []*RaftServer{}

	for i := 1; i <= noOfServers; i++ {

		id := i
		address := fmt.Sprintf(":%d", 9000+id)

		allMembers := []Member{}

		for i := (1); i <= noOfServers; i++ {
			if i == id {
				continue
			}

			member := Member{Address: fmt.Sprintf(":%d", 9000+i)}

			allMembers = append(allMembers, member)
		}

		log.Printf("Id is %d", id)
		raftServer := NewRaftServer(Config{
			Id:          uint64(id),
			Address:     address,
			HeartBeatMs: uint64(heartbeatTimeoutMs),
			Members:     allMembers,
		})

		raftServer.Start()

		allRaftServer = append(allRaftServer, raftServer)
	}

	return allRaftServer
}
