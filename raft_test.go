package goraft

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPersist(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_persist")

	require.NoError(t, err)

	defer os.Remove(tempFile.Name())

	require.NoError(t, err)

	raftServer := NewRaftServer(tempFile, Config{})

	allEntry := []Entry{
		{term: 0, command: []byte("Hello world")},
		{term: 0, command: []byte("Hello world 2")},
		{term: 0, command: []byte("Hello world 3")},
	}

	raftServer.log = allEntry
	raftServer.persist(3)

	wantFileContent := []byte{}

	page := encodePage(0, 0)

	wantFileContent = append(wantFileContent, page[:]...)
	for _, entry := range allEntry {
		encodedEntry := encodeEntry(&entry)

		wantFileContent = append(wantFileContent, encodedEntry[:]...)
	}

	tempFileStat, err := os.Stat(tempFile.Name())

	require.NoError(t, err)

	fileSize := tempFileStat.Size()

	actualFileContent := make([]byte, fileSize)

	_, err = tempFile.Seek(0, io.SeekStart)

	require.NoError(t, err)

	_, err = tempFile.Read(actualFileContent)

	require.NoError(t, err)

	require.Equal(t, wantFileContent, actualFileContent)
}

func TestRestore(t *testing.T) {
	scenarios := map[string]func(*testing.T){
		"Restoring on first boot. (the file is empty)": testRestoringOnFirstBoot,
		"Restoring persisted data":                     testRestoringPersistedData,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, fn)
	}

}

func testRestoringOnFirstBoot(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-restore")

	require.NoError(t, err)

	defer os.Remove(tempFile.Name())

	raftServer := NewRaftServer(tempFile, Config{})

	raftServer.restore()

	require.Equal(t, uint64(0), raftServer.currentTerm)
	require.Equal(t, uint64(0), raftServer.votedFor)
	require.Equal(t, 0, len(raftServer.log))
}

func testRestoringPersistedData(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-restore")

	fileName := tempFile.Name()

	require.NoError(t, err)

	defer os.Remove(fileName)

	raftServer := NewRaftServer(tempFile, Config{})

	entries := []Entry{
		{term: 0, command: []byte("First Log")},
		{term: 0, command: []byte("Second log")},
		{term: 0, command: []byte("Third log")},
	}

	raftServer.log = entries
	raftServer.currentTerm = 1
	raftServer.votedFor = 2

	raftServer.persist(len(entries))

	tempFile.Close()

	tempFile, err = os.OpenFile(fileName, os.O_SYNC|os.O_RDWR, 0755)

	require.NoError(t, err)

	raftServer = NewRaftServer(tempFile, Config{})

	raftServer.restore()

	require.Equal(t, uint64(1), raftServer.currentTerm)
	require.Equal(t, uint64(2), raftServer.votedFor)

	require.Equal(t, entries, raftServer.log)
}

func TestElectionTimer(t *testing.T) {
	tempFile, err := os.CreateTemp("", "election-timer")

	require.NoError(t, err)

	defer os.Remove(tempFile.Name())

	heartbeatMs := 20
	raftServer := NewRaftServer(tempFile, Config{heartbeatMs: heartbeatMs})

	t.Log("Raft Server started")
	raftServer.Start()

	timeout := time.After(time.Duration(heartbeatMs*2+heartbeatMs) * time.Millisecond)
	ticker := time.NewTicker(1 * time.Millisecond)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if raftServer.state.Get() != leaderState {
				continue
			}
		case <-timeout:
			break
		}

		break

	}

	require.Equal(t, candidateState, raftServer.state.Get())
	require.Equal(t, uint64(1), raftServer.currentTerm)
}
