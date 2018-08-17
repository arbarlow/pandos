package main

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/arbarlow/pandos/log"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	verbose = kingpin.Flag(
		"log level",
		"The log level, one of DEBUG, INFO, ERROR",
	).Short('v').Default("INFO").Enum("DEBUG", "INFO", "ERROR")

	storageDir = kingpin.Flag(
		"storage directory",
		"the directory for storing ranges",
	).Short('s').Default("./").String()
	storage, _ = filepath.Abs(*storageDir)

	hostname, _   = os.Hostname()
	advertiseHost = kingpin.Flag(
		"advertiseHost",
		"The hostname to advertise to other nodes",
	).Short('h').Default(hostname).String()

	port = kingpin.Flag(
		"port",
		"the port for gRPC based communication (and node chat)",
	).Short('p').Default("8000").String()

	peer = kingpin.Flag(
		"peer",
		"a node to target to join a cluster",
	).Short('j').String()
)

func main() {
	kingpin.Parse()

	l := log.Logger()
	defer l.Sync()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := r.Uint64()

	log.PrintStartupInfo(strconv.FormatUint(id, 10), *verbose, storage,
		*advertiseHost, *port, *peer)

	// getSnapshot := func() ([]byte, error) { return []byte{}, nil }

	// proposeC := make(chan string)
	// defer close(proposeC)
	// confChangeC := make(chan raftpb.ConfChange)
	// defer close(confChangeC)

	// _, errorC, _ := raft.NewRaftNode(
	// 	int(id),
	// 	[]string{*peer},
	// 	(peer != nil),
	// 	getSnapshot,
	// 	proposeC,
	// 	confChangeC,
	// )

	// <-errorC
}
