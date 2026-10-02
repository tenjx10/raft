package raft

// A TCP transport for Raft built on Go's net/rpc package.
//
// Each Raft server runs a Server that accepts RequestVote and AppendEntries
// RPCs, and holds one Peer per other server for sending them. Arguments are
// encoded with encoding/gob, so any custom command type passed to Start()
// must be registered with gob.Register() first (built-in types like string
// and int already are).

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	// how long to wait when connecting to a peer
	dialTimeout = 100 * time.Millisecond

	// how long to wait for a reply before treating the RPC as lost; kept
	// well below the 300ms minimum election timeout
	callTimeout = 200 * time.Millisecond
)

// rpcHandler adapts Raft's RPC handlers to the signature net/rpc requires.
type rpcHandler struct {
	rf *Raft
}

func (h *rpcHandler) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	h.rf.RequestVote(args, reply)
	return nil
}

func (h *rpcHandler) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	h.rf.AppendEntries(args, reply)
	return nil
}

// Server receives RPCs from other Raft servers over TCP.
type Server struct {
	listener net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]bool
	closed bool
}

// Listen binds addr (e.g. "localhost:8000", or "localhost:0" for any free
// port). It does not accept connections until Serve is called, which lets
// callers learn every server's address before creating the Raft peers.
func Listen(addr string) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{listener: l, conns: make(map[net.Conn]bool)}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Serve starts delivering incoming RPCs to rf. It returns immediately.
func (s *Server) Serve(rf *Raft) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &rpcHandler{rf: rf}); err != nil {
		panic(err)
	}

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				return // listener closed
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				conn.Close()
				return
			}
			s.conns[conn] = true
			s.mu.Unlock()

			go func() {
				srv.ServeConn(conn)
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
			}()
		}
	}()
}

// Close stops accepting RPCs and drops all open connections, so to the rest
// of the cluster this server looks crashed.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.listener.Close()
	for conn := range s.conns {
		conn.Close()
	}
}

// tcpPeer is a Peer that sends RPCs to the Server at addr. It connects
// lazily and reconnects after failures, so peers can be created before the
// other servers are up.
type tcpPeer struct {
	addr string

	mu     sync.Mutex
	client *rpc.Client
}

// NewPeer returns a Peer that sends RPCs to the Server listening on addr.
func NewPeer(addr string) Peer {
	return &tcpPeer{addr: addr}
}

func (p *tcpPeer) connect() *rpc.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		conn, err := net.DialTimeout("tcp", p.addr, dialTimeout)
		if err != nil {
			return nil
		}
		p.client = rpc.NewClient(conn)
	}
	return p.client
}

// drop closes a broken connection so the next Call redials.
func (p *tcpPeer) drop(client *rpc.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == client {
		p.client.Close()
		p.client = nil
	}
}

// Call sends an RPC and waits up to callTimeout for the reply. It returns
// false if the server is unreachable or doesn't answer in time.
func (p *tcpPeer) Call(method string, args interface{}, reply interface{}) bool {
	client := p.connect()
	if client == nil {
		return false
	}

	call := client.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			p.drop(client)
			return false
		}
		return true
	case <-time.After(callTimeout):
		return false
	}
}
