package goraft

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type Entry struct {
	command []byte
	term    uint64
}

type RaftServer struct {
	// Perist state of a Rafe server
	currentTerm uint64
	votedFor    uint64
	log         []Entry

	fd *os.File
}

func NewRaftServer(fd *os.File) *RaftServer {
	return &RaftServer{currentTerm: 0, votedFor: 0, log: nil, fd: fd}
}

/**
 * 0:8 => currentTerm
 * 8:16 => votedFor
 */
const PAGE_SIZE = 4096

func encodePage(currentTerm uint64, votedFor uint64) [PAGE_SIZE]byte {
	var page [PAGE_SIZE]byte

	binary.LittleEndian.PutUint64(page[0:8], currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], votedFor)

	return page
}

// Maximum size of a single command in log entry
const ENTRY_COMMAND_SIZE = 1024

/**
 * 0:8 => Term for entry
 * 8:16 => Size of entry command
 * 16:N => Entry command
 */
const ENTRY_HEADER_SIZE = 16

/**
 * Total size of Entry
 */
const ENTRY_SIZE = ENTRY_COMMAND_SIZE + ENTRY_HEADER_SIZE

func encodeEntry(entry *Entry) [ENTRY_SIZE]byte {
	term := entry.term
	sizeOfCommand := len(entry.command)

	if sizeOfCommand > ENTRY_COMMAND_SIZE {
		panic("Command size is greater than ENTRY_COMMAND_SIZE")
	}

	var encodedEntry [ENTRY_SIZE]byte

	binary.LittleEndian.PutUint64(encodedEntry[0:8], term)
	binary.LittleEndian.PutUint64(encodedEntry[8:16], uint64(sizeOfCommand))
	copy(encodedEntry[16:16+sizeOfCommand], entry.command)

	return encodedEntry
}

func (r *RaftServer) persist(nNewEntries int) {
	page := encodePage(r.currentTerm, r.votedFor)
	if _, err := r.fd.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	if _, err := r.fd.Write(page[:]); err != nil {
		panic(err)
	}

	if nNewEntries > 0 {
		newLogOffset := max(len(r.log)-nNewEntries, 0)

		if _, err := r.fd.Seek(int64(PAGE_SIZE+newLogOffset*ENTRY_SIZE), 0); err != nil {
			panic(err)
		}

		bw := bufio.NewWriter(r.fd)

		for i := newLogOffset; i < len(r.log); i++ {
			entry := encodeEntry(&r.log[i])

			if _, err := bw.Write(entry[:]); err != nil {
				panic(err)
			}

		}

		if err := bw.Flush(); err != nil {
			panic(err)
		}

		if err := r.fd.Sync(); err != nil {
			panic(err)
		}
	}
}
