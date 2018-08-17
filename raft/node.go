package raft

import (
	"os"

	"github.com/arbarlow/pandos/log"
	"github.com/arbarlow/pandos/raft/transport"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
)

var defaultSnapshotCount uint64 = 10000

type raftNode struct {
	node        raft.Node
	raftStorage raft.Storage

	dir string // path to storage directory

	proposeC    <-chan string            // proposed message to log
	confChangeC <-chan raftpb.ConfChange // proposed config changes
	errorC      chan<- error             // errors from raft session

	id    uint64   // client ID for raft session
	peers []string // raft peer URLs (FQDN)

	join        bool   // node is joining an existing cluster
	lastIndex   uint64 // index of log at start
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	snapCount uint64 // threshold for when to do snapshots
	transport *transport.Transport
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
	confChangeC <-chan raftpb.ConfChange) (
	<-chan error,
	<-chan bool) {

	errorC := make(chan error)
	readyC := make(chan bool)

	// rc := &raftNode{
	// 	id:    id,
	// 	dir:   fmt.Sprintf("data/%d", id),
	// 	peers: peers,
	// 	join:  join,

	// 	proposeC: proposeC,
	// 	errorC:   errorC,

	// 	snapCount: defaultSnapshotCount,
	// 	stopc:     make(chan struct{}),
	// }

	// go rc.startRaft()
	return errorC, readyC
}

// func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
// 	// must save the snapshot index to the WAL before saving the
// 	// snapshot to maintain the invariant that we only Open the
// 	// wal at previously-saved snapshot indexes.
// 	walSnap := walpb.Snapshot{
// 		Index: snap.Metadata.Index,
// 		Term:  snap.Metadata.Term,
// 	}
// 	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
// 		return err
// 	}
// 	if err := rc.snapshotter.SaveSnap(snap); err != nil {
// 		return err
// 	}
// 	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
// }

// func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
// 	if len(ents) == 0 {
// 		return
// 	}
// 	firstIdx := ents[0].Index
// 	if firstIdx > rc.appliedIndex+1 {
// 		log.Logger().Fatal(
// 			"first index of committed entry[idx] should <= progress.appliedIndex[applied idx]+1",
// 			zap.Uint64("idx", firstIdx),
// 			zap.Uint64("applied idx", rc.appliedIndex),
// 		)
// 	}
// 	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
// 		nents = ents[rc.appliedIndex-firstIdx+1:]
// 	}
// 	return nents
// }

// // publishEntries writes committed log entries to commit channel and returns
// // whether all entries could be published.
// func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
// 	for i := range ents {
// 		switch ents[i].Type {
// 		case raftpb.EntryNormal:
// 			if len(ents[i].Data) == 0 {
// 				// ignore empty messages
// 				break
// 			}
// 			s := string(ents[i].Data)
// 			select {
// 			case rc.commitC <- &s:
// 			case <-rc.stopc:
// 				return false
// 			}

// 		case raftpb.EntryConfChange:
// 			var cc raftpb.ConfChange
// 			cc.Unmarshal(ents[i].Data)
// 			rc.confState = *rc.node.ApplyConfChange(cc)
// 			switch cc.Type {
// 			case raftpb.ConfChangeAddNode:
// 				if len(cc.Context) > 0 {
// 					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
// 				}
// 			case raftpb.ConfChangeRemoveNode:
// 				if cc.NodeID == uint64(rc.id) {
// 					log.Logger().Info("I've been removed from the cluster! Shutting down.")
// 					return false
// 				}
// 				rc.transport.RemovePeer(types.ID(cc.NodeID))
// 			}
// 		}

// 		// after commit, update appliedIndex
// 		rc.appliedIndex = ents[i].Index

// 		// special nil commit to signal replay has finished
// 		if ents[i].Index == rc.lastIndex {
// 			select {
// 			case rc.commitC <- nil:
// 			case <-rc.stopc:
// 				return false
// 			}
// 		}
// 	}
// 	return true
// }

// func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
// 	snapshot, err := rc.snapshotter.Load()
// 	if err != nil && err != snap.ErrNoSnapshot {
// 		log.Logger().Fatal("raftexample: error loading snapshot", zap.Error(err))
// 	}
// 	return snapshot
// }

// // openWAL returns a WAL ready for reading.
// func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
// 	if !wal.Exist(rc.waldir) {
// 		if err := os.MkdirAll(rc.waldir, 0750); err != nil {
// 			log.Logger().Fatal("raftexample: cannot create dir for wal", zap.Error(err))
// 		}

// 		w, err := wal.Create(log.Logger(), rc.waldir, nil)
// 		if err != nil {
// 			log.Logger().Fatal("raftexample: create wal error", zap.Error(err))
// 		}
// 		w.Close()
// 	}

// 	walsnap := walpb.Snapshot{}
// 	if snapshot != nil {
// 		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
// 	}
// 	log.Logger().Info(fmt.Sprintf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index))
// 	w, err := wal.Open(log.Logger(), rc.waldir, walsnap)
// 	if err != nil {
// 		log.Logger().Info("raftexample: error loading wal", zap.Error(err))
// 	}

// 	return w
// }

// // replayWAL replays WAL entries into the raft instance.
// func (rc *raftNode) replayWAL() *wal.WAL {
// 	log.Logger().Info("replaying WAL of member", zap.Int("member", rc.id))
// 	snapshot := rc.loadSnapshot()
// 	w := rc.openWAL(snapshot)
// 	_, st, ents, err := w.ReadAll()
// 	if err != nil {
// 		log.Logger().Info("raftexample: failed to read WAL", zap.Error(err))
// 	}
// 	rc.raftStorage = raft.NewMemoryStorage()
// 	if snapshot != nil {
// 		rc.raftStorage.ApplySnapshot(*snapshot)
// 	}
// 	rc.raftStorage.SetHardState(st)

// 	// append to storage so raft starts at the right place in log
// 	rc.raftStorage.Append(ents)
// 	// send nil once lastIndex is published so client knows commit channel is current
// 	if len(ents) > 0 {
// 		rc.lastIndex = ents[len(ents)-1].Index
// 	} else {
// 		rc.commitC <- nil
// 	}
// 	return w
// }

// func (rc *raftNode) writeError(err error) {
// 	rc.stopHTTP()
// 	close(rc.commitC)
// 	rc.errorC <- err
// 	close(rc.errorC)
// 	rc.node.Stop()
// }

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.dir) {
		if err := os.MkdirAll(rc.dir, 0750); err != nil {
			log.Logger().Error(
				"raft: cannot create dir for range",
				zap.Error(err),
				zap.Uint64("range", rc.id),
			)
		}
	}

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	_ = &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   64 * (1024 * 1024),
		MaxInflightMsgs: 256,
	}

	// if oldwal {
	// 	rc.node = raft.RestartNode(c)
	// } else {
	// 	startPeers := rpeers
	// 	if rc.join {
	// 		startPeers = nil
	// 	}
	// 	rc.node = raft.StartNode(c, startPeers)
	// }

	// rc.transport = &rafthttp.Transport{
	// 	Logger:      log.Logger(),
	// 	ID:          types.ID(rc.id),
	// 	ClusterID:   0x1000,
	// 	Raft:        rc,
	// 	ServerStats: stats.NewServerStats("", ""),
	// 	LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
	// 	ErrorC:      make(chan error),
	// }

	// rc.transport.Start()
	// for i := range rc.peers {
	// 	if i+1 != rc.id {
	// 		rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
	// 	}
	// }

	// 	go rc.serveRaft()
	// 	go rc.serveChannels()
}

// // stop closes http, closes all channels, and stops raft.
// func (rc *raftNode) stop() {
// 	rc.stopHTTP()
// 	close(rc.commitC)
// 	close(rc.errorC)
// 	rc.node.Stop()
// }

// func (rc *raftNode) stopHTTP() {
// 	rc.transport.Stop()
// 	close(rc.httpstopc)
// 	<-rc.httpdonec
// }

// func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
// 	if raft.IsEmptySnap(snapshotToSave) {
// 		return
// 	}

// 	log.Logger().Info("publishing snapshot at index", zap.Uint64("snapshot", rc.snapshotIndex))
// 	defer log.Logger().Info("finished publishing snapshot at index", zap.Uint64("index", rc.snapshotIndex))

// 	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
// 		log.Logger().Fatal(fmt.Sprintln(
// 			"snapshot index [%d] should > progress.appliedIndex [%d] + 1",
// 			snapshotToSave.Metadata.Index,
// 			rc.appliedIndex,
// 		))
// 	}
// 	rc.commitC <- nil // trigger kvstore to load snapshot

// 	rc.confState = snapshotToSave.Metadata.ConfState
// 	rc.snapshotIndex = snapshotToSave.Metadata.Index
// 	rc.appliedIndex = snapshotToSave.Metadata.Index
// }

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
// 	data, err := rc.getSnapshot()
// 	if err != nil {
// 		panic(err)
// 	}

// 	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
// 	if err != nil {
// 		panic(err)
// 	}

// 	if err := rc.saveSnap(snap); err != nil {
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

// func (rc *raftNode) serveChannels() {
// 	snap, err := rc.raftStorage.Snapshot()
// 	if err != nil {
// 		panic(err)
// 	}
// 	rc.confState = snap.Metadata.ConfState
// 	rc.snapshotIndex = snap.Metadata.Index
// 	rc.appliedIndex = snap.Metadata.Index

// 	defer rc.wal.Close()

// 	ticker := time.NewTicker(100 * time.Millisecond)
// 	defer ticker.Stop()

// 	// send proposals over raft
// 	go func() {
// 		var confChangeCount uint64 = 0

// 		for rc.proposeC != nil && rc.confChangeC != nil {
// 			select {
// 			case prop, ok := <-rc.proposeC:
// 				if !ok {
// 					rc.proposeC = nil
// 				} else {
// 					// blocks until accepted by raft state machine
// 					rc.node.Propose(context.TODO(), []byte(prop))
// 				}

// 			case cc, ok := <-rc.confChangeC:
// 				if !ok {
// 					rc.confChangeC = nil
// 				} else {
// 					confChangeCount += 1
// 					cc.ID = confChangeCount
// 					rc.node.ProposeConfChange(context.TODO(), cc)
// 				}
// 			}
// 		}
// 		// client closed channel; shutdown raft if not already
// 		close(rc.stopc)
// 	}()

// 	// event loop on raft state machine updates
// 	for {
// 		select {
// 		case <-ticker.C:
// 			rc.node.Tick()

// 		// store raft entries to wal, then publish over commit channel
// 		case rd := <-rc.node.Ready():
// 			rc.wal.Save(rd.HardState, rd.Entries)
// 			if !raft.IsEmptySnap(rd.Snapshot) {
// 				rc.saveSnap(rd.Snapshot)
// 				rc.raftStorage.ApplySnapshot(rd.Snapshot)
// 				rc.publishSnapshot(rd.Snapshot)
// 			}
// 			rc.raftStorage.Append(rd.Entries)
// 			rc.transport.Send(rd.Messages)
// 			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
// 				rc.stop()
// 				return
// 			}
// 			rc.maybeTriggerSnapshot()
// 			rc.node.Advance()

// 		case err := <-rc.transport.ErrorC:
// 			rc.writeError(err)
// 			return

// 		case <-rc.stopc:
// 			rc.stop()
// 			return
// 		}
// 	}
// }

// func (rc *raftNode) serveRaft() {
// 	url, err := url.Parse(rc.peers[rc.id-1])
// 	if err != nil {
// 		log.Logger().Info("raftexample: Failed parsing URL", zap.Error(err))
// 	}

// 	ln, err := newStoppableListener(url.Host, rc.httpstopc)
// 	if err != nil {
// 		log.Logger().Info("raftexample: Failed to listen rafthttp", zap.Error(err))
// 	}

// 	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
// 	select {
// 	case <-rc.httpstopc:
// 	default:
// 		log.Logger().Fatal("raftexample: Failed to serve rafthttp", zap.Error(err))
// 	}
// 	close(rc.httpdonec)
// }

// func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
// 	return rc.node.Step(ctx, m)
// }
// func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
// func (rc *raftNode) ReportUnreachable(id uint64)                          {}
// func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// // stoppableListener sets TCP keep-alive timeouts on accepted
// // connections and waits on stopc message
// type stoppableListener struct {
// 	*net.TCPListener
// 	stopc <-chan struct{}
// }

// func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
// 	ln, err := net.Listen("tcp", addr)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
// }
