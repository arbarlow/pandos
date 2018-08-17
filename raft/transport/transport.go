package transport

import (
	"sync"

	"go.uber.org/zap"
)

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
type Transport struct {
	Logger *zap.Logger

	// ID          types.ID   // local member ID
	// URLs        types.URLs // local peer URLs
	// ClusterID   types.ID   // raft cluster ID for request validation

	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	mu sync.RWMutex // protect the remote and peer map
}
