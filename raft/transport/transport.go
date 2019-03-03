package transport

import (
	"context"
	"fmt"
	"net"

	"github.com/arbarlow/pandos/pandos"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
type Transport struct {
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error
}

// Send sends out the given messages to the remote peers.
// Each message has a To field, which is an id that maps
// to an existing peer in the transport.
// If the id cannot be found in the transport, the message
// will be ignored.
func (t *Transport) Send(m []raftpb.Message) {

}

// AddPeer adds a peer with given peer urls into the transport.
// It is the caller's responsibility to ensure the urls are all valid,
// or it panics.
// Peer urls are used to connect to the remote peer.
func (t *Transport) AddPeer(id types.ID, urls []string) {

}

// RemovePeer removes the peer with given id.
func (t *Transport) RemovePeer(id types.ID) {

}

type gossipServer struct{}

func (s *gossipServer) Receive(ctx context.Context, m *raftpb.Message) (*empty.Empty, error) {
	return nil, nil
}

func Start(port string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pandos.RegisterPandosGossipServer(grpcServer, &gossipServer{})
	return grpcServer.Serve(lis)
}
