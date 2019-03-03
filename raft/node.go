package raft

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/arbarlow/pandos/log"
	"github.com/arbarlow/pandos/raft/transport"
	"github.com/arbarlow/pandos/storage"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.uber.org/zap"
)

var defaultSnapshotCount uint64 = 10000

type raftNode struct {
	node      raft.Node
	storage   *storage.LevelDB
	transport *transport.Transport
	logger    *zap.Logger

	proposeC    <-chan string            // proposed message to log
	confChangeC <-chan raftpb.ConfChange // proposed config changes
	errorC      chan<- error             // errors from raft session

	id    uint64   // client ID for raft session
	peers []string // raft peer URLs (FQDN)

	join bool // node is joining an existing cluster

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64
	lastIndex     uint64

	snapCount uint64        // threshold for when to do snapshots
	stopc     chan struct{} // signals proposal channel closed
}

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel.
//
// To shutdown, close proposeC and read errorC.
func NewRaftNode(
	id uint64,
	peers []string,
	join bool,
	proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) <-chan error {

	l := log.Logger()
	errorC := make(chan error)
	dir := fmt.Sprintf("data/%d", id)
	db, err := storage.NewLevelDB(dir)
	if err != nil {
		errorC <- err
		return errorC
	}

	rc := &raftNode{
		id:      id,
		peers:   peers,
		join:    join,
		storage: db,
		logger:  l,

		proposeC: proposeC,
		errorC:   errorC,

		snapCount: defaultSnapshotCount,
		stopc:     make(chan struct{}),
	}

	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.startRaft()
	return errorC
}

func (rc *raftNode) startRaft() {
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	c := &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.storage,
		MaxSizePerMsg:   64 * (1024 * 1024),
		MaxInflightMsgs: 256,
	}

	// TODO: open previous db and join
	hasData := false
	if hasData {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.storage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.storage.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			if !raft.IsEmptySnap(rd.Snapshot) {
				err := rc.storage.LoadSnapshot(rd.Snapshot)
				if err != nil {
					rc.writeError(err)
					return
				}

				rc.publishSnapshot(rd.Snapshot)
			}

			rc.storage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			// rc.maybeTriggerSnapshot()
			rc.node.Advance()
		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return
		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		rc.logger.Fatal(
			"first index of committed entry[idx] should <= progress.appliedIndex[applied idx]+1",
			zap.Uint64("idx", firstIdx),
			zap.Uint64("applied idx", rc.appliedIndex),
		)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			// s := string(ents[i].Data)
			select {
			// case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Logger().Info("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			// case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) writeError(err error) {
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	rc.logger.Info("publishing snapshot at index", zap.Uint64("snapshot", rc.snapshotIndex))
	defer rc.logger.Info("finished publishing snapshot at index", zap.Uint64("index", rc.snapshotIndex))

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		rc.logger.Fatal(fmt.Sprintf(
			"snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			snapshotToSave.Metadata.Index,
			rc.appliedIndex,
		))
	}

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

// var snapshotCatchUpEntriesN uint64 = 10000

// func (rc *raftNode) maybeTriggerSnapshot() {
// 	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
// 		return
// 	}

// 	log.Logger().Info(fmt.Sprintf(
// 		"start snapshot [applied index: %d | last snapshot index: %d]",
// 		rc.appliedIndex,
// 		rc.snapshotIndex,
// 	))

// 	data, err := rc.storage.Snapshot()
// 	if err != nil {
// 		panic(err)
// 	}

// 	compactIndex := uint64(1)
// 	if rc.appliedIndex > snapshotCatchUpEntriesN {
// 		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
// 	}

// 	if err := rc.raftStorage.Compact(compactIndex); err != nil {
// 		panic(err)
// 	}

// 	log.Logger().Info("compacted log at index", zap.Uint64("index", compactIndex))
// 	rc.snapshotIndex = rc.appliedIndex
// }

// func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
// 	return rc.node.Step(ctx, m)
// }
// func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
// func (rc *raftNode) ReportUnreachable(id uint64)                          {}
// func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
