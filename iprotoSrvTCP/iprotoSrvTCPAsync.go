/*
	Asynchronous mail.ru 'iproto' protocol implementation on Go.
	ServerTCP Asynchronous

	Protocol description
	<request> | <response> := <header><body>
	<header> = <type:int32><body_length:int32><request_id:int32>

	(c) 2013, 2014 Cergoo
	under terms of ISC license

*/
package iprotoSrvTCPAsync

import (
	"encoding/binary"
	"log"
	"net"
	"time"
)

type (
	// handler interface
	Handler interface {
		Work(*IprotoPack) *IprotoPack
	}

	// server struct
	Server struct {
		log       *log.Logger    //
		addr      string         //
		chStop    chan bool      // chanel to stop all gorutines
		chRequest chan *tRequest //
		handler   Handler        //
		Clients   *tClients      // connections
	}

	Client struct {
		Datetime     time.Time       // time of connect
		Conn         *net.TCPConn    // connect
		ChRespondent chan *tResponse // chan to respondent
	}

	tResponse struct {
		Header []byte
		Body   []byte
	}

	tRequest struct {
		ch   chan<- *tResponse
		pack *IprotoPack
	}

	/*
		requestType = header[0]
		bodyLength  = header[1]
		requestID   = header[2]
	*/
	IprotoPack struct {
		Header [3]int32
		Body   []byte
	}
)

const deadLine = 30 * time.Second

// New constructor create new server
func New(
	addr string,
	handler Handler,
	log *log.Logger,
	workersCount uint8,
) (srv *Server) {
	srv = &Server{
		addr:      addr,
		handler:   handler,
		log:       log,
		Clients:   newClients(),
		chStop:    make(chan bool, 1),
		chRequest: make(chan *tRequest, workersCount),
	}

	return
}

// Run run server
func (t *Server) Run() (e error) {
	var (
		laddr   *net.TCPAddr
		listner *net.TCPListener
	)

	laddr, e = net.ResolveTCPAddr("tcp", t.addr)
	if e != nil {
		return
	}
	listner, e = net.ListenTCP("tcp", laddr)
	if e != nil {
		return
	}

	go t.listner(listner)
	for i := 0; i <= len(t.chRequest); i++ {
		go t.work()
	}
	return
}

// Stop stop server
func (t *Server) Stop() {
	// stop listner
	t.chStop <- true
	// stop all workers
	close(t.chRequest)
	// close all net connections
	for _, v := range t.Clients.data {
		t.CloseClient(v)
	}
}

// CloseClient close client conection
func (t *Server) CloseClient(client *Client) {
	t.Clients.close(client)
	t.log.Println("client close: " + client.Conn.RemoteAddr().String())
}

func (t *Server) listner(listner *net.TCPListener) {
	t.log.Println("listner start: " + t.addr)
	defer func() {
		t.log.Println("listner stopped: " + t.addr)
	}()

	var (
		ok     bool
		err    error
		neterr net.Error
		conn   *net.TCPConn
	)

	listner.SetDeadline(time.Now().Add(deadLine))

	for {
		conn, err = listner.AcceptTCP()

		if err == nil {
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(10 * time.Minute)

			client := &Client{
				Datetime:     time.Now(),
				Conn:         conn,
				ChRespondent: make(chan *tResponse, 4),
			}

			t.Clients.Set(client)
			go t.requester(client)
			go t.respondent(client)
		} else if neterr, ok = err.(net.Error); ok && neterr.Timeout() {
			listner.SetDeadline(time.Now().Add(deadLine))
		} else {
			t.log.Println(err)
		}
		select {
		case <-t.chStop:
			return
		}
	}
}

func (t *Server) requester(client *Client) {
	t.log.Panicln("requester start: " + client.Conn.RemoteAddr().String())
	defer func() {
		t.log.Panicln("requester stopped: " + client.Conn.RemoteAddr().String())
		if e := recover(); e != nil {
			t.CloseClient(client)
		}
	}()

	var (
		e      error
		header [3]int32
	)

	headerBuf := make([]byte, 12)

	for {
		_, e = client.Conn.Read(headerBuf)
		if e != nil {
			t.log.Panicln(e)
		}

		header[0] = int32(binary.LittleEndian.Uint32(headerBuf[:4]))
		header[1] = int32(binary.LittleEndian.Uint32(headerBuf[4:8]))
		header[2] = int32(binary.LittleEndian.Uint32(headerBuf[8:]))
		bodyBuf := make([]byte, header[1])

		_, e = client.Conn.Read(bodyBuf)
		if e != nil {
			t.log.Panicln(e)
		}

		t.chRequest <- &tRequest{ch: client.ChRespondent, pack: &IprotoPack{
			Header: header,
			Body:   bodyBuf,
		}}

	}
}

func (t *Server) respondent(client *Client) {
	t.log.Println("respondent start: " + client.Conn.RemoteAddr().String())
	defer func() {
		t.log.Println("respondent stopped: " + client.Conn.RemoteAddr().String())
		if e := recover(); e != nil {
			t.CloseClient(client)
		}
	}()

	var (
		v *tResponse
		e error
	)

	for v = range client.ChRespondent {
		_, e = client.Conn.Write(v.Header)
		if e != nil {
			t.log.Panicln(e)
		}
		_, e = client.Conn.Write(v.Body)
		if e != nil {
			t.log.Panicln(e)
		}
	}
}

func (t *Server) work() {
	t.log.Println("worker start")
	defer func() {
		if e := recover(); e != nil {
			t.log.Println("worker error: ", e)
			t.log.Println("worker stop")
			go t.work()
		} else {
			t.log.Println("worker stop")
		}
	}()

	var (
		v       *tRequest
		request *IprotoPack
	)

	for v = range t.chRequest {
		request = t.handler.Work(v.pack)
		if request != nil {
			headerBuf := make([]byte, 12)
			binary.LittleEndian.PutUint32(headerBuf, uint32(request.Header[0]))
			binary.LittleEndian.PutUint32(headerBuf[4:], uint32(len(request.Body)))
			binary.LittleEndian.PutUint32(headerBuf[8:], uint32(request.Header[2]))
			v.ch <- &tResponse{headerBuf, request.Body}
		}
	}
}
