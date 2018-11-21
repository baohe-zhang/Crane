package messages

import (
	"net"
	"sync"
)

// Thread safe pool of client connections
type ConnPool struct {
	pool   map[string]net.Conn
	RWLock sync.RWMutex
}

// New ConnPool for transporting messages
func NewConnPool() *ConnPool {
	cp := &ConnPool{}
	cp.pool = make(map[string]net.Conn)
	return cp
}

// Iteration over ConnPool
func (cp *ConnPool) Range(callback func(string, net.Conn)) {
	cp.RWLock.RLock()
	for id, conn := range cp.pool {
		callback(id, conn)
	}
	cp.RWLock.RUnlock()
}

// Get the connection by connection id
func (cp *ConnPool) Get(id string) net.Conn {
	cp.RWLock.RLock()
	conn := cp.pool[id]
	cp.RWLock.RUnlock()
	return conn
}

func (cp *ConnPool) Insert(id string, conn net.Conn) {
	cp.RWLock.Lock()
	cp.pool[id] = conn
	cp.RWLock.Unlock()
	return
}

func (cp *ConnPool) Delete(id string) {
	cp.RWLock.Lock()
	delete(cp.pool, id)
	cp.RWLock.Unlock()
	return
}

func (cp *ConnPool) Size() int {
	return len(cp.pool)
}
