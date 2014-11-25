package iprotoSrvTCPAsync

import (
	"sync"
	"time"
)

type (
	tClients struct {
		data map[string]*Client
		m    sync.RWMutex
	}
)

func newClients() *tClients {
	return &tClients{
		data: make(map[string]*Client),
	}
}

// Set set new client
func (t *tClients) Set(client *Client) {
	t.m.Lock()
	t.data[client.Conn.RemoteAddr().String()] = client
	t.m.Unlock()
}

// Del del client
func (t *tClients) Del(client *Client) {
	t.m.Lock()
	delete(t.data, client.Conn.RemoteAddr().String())
	t.m.Unlock()
}

// List get a list clients
func (t *tClients) List() (r [][2]string) {
	t.m.RLock()
	for i, v := range t.data {
		r = append(r, [2]string{i, v.Datetime.Format(time.RFC1123)})
	}
	t.m.RUnlock()
	return
}

func (t *tClients) close(client *Client) {
	close(client.ChRespondent)
	t.Del(client)
	client.Conn.Close()
}
