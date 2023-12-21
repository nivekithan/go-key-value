package goraft

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersist(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_persist")

	require.NoError(t, err)

	defer os.Remove(tempFile.Name())

	require.NoError(t, err)

	raftServer := NewRaftServer(tempFile)

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
