/*
	Asynchronous mail.ru iproto protocol implementation on Go.
	Thread safe async client

	Protocol description
	<request> | <response> := <header><body>
	<header> = <type:int32><body_length:int32><request_id:int32>

	(c) 2013 Cergoo (forked from fl00r/go-iproto)
	under terms of ISC license

*/
package iprotoClient

import (
	"encoding/binary"
	"net"
	"runtime"
)

const chanLen = 10

type (
	// Client main struct
	Client struct {
		conn      net.Conn         //
		frameSize uint16           //
		ChRead    chan *IprotoPack //
		ChWrite   chan *IprotoPack //
	}

	IprotoPack struct {
		Header [3]int32
		Body   []byte
	}
)

// New constructor of a new server
func Connect(addr string) (t *Client, err error) {

	t = &Client{
		ChRead:  make(chan *IprotoPack, chanLen),
		ChWrite: make(chan *IprotoPack, chanLen),
	}

	t.conn, err = net.Dial("tcp", addr)
	if err != nil {
		return
	}

	go t.reader()
	go t.writer()

	// destroy action
	stopAllGorutines := func(t *Client) {
		close(t.ChWrite)
		t.conn.Close()
	}
	runtime.SetFinalizer(t, stopAllGorutines)
	return
}

func (t *Client) reader() {
	var (
		e      error
		header [3]int32
	)

	for {
		headerBuf := make([]byte, 12)
		_, e = t.conn.Read(headerBuf)
		if e != nil {
			return
		}

		header[0] = int32(binary.LittleEndian.Uint32(headerBuf[:4]))
		header[1] = int32(binary.LittleEndian.Uint32(headerBuf[4:8]))
		header[2] = int32(binary.LittleEndian.Uint32(headerBuf[8:]))
		bodyBuf := make([]byte, header[1])

		_, e = t.conn.Read(bodyBuf)
		if e != nil {
			return
		}

		t.ChRead <- &IprotoPack{
			Header: header,
			Body:   bodyBuf,
		}
	}
}

func (t *Client) writer() {
	var (
		e error
		v *IprotoPack
	)
	headerBuf := make([]byte, 12)

	for v = range t.ChWrite {
		binary.LittleEndian.PutUint32(headerBuf, uint32(v.Header[0]))
		binary.LittleEndian.PutUint32(headerBuf[4:], uint32(len(v.Body)))
		binary.LittleEndian.PutUint32(headerBuf[8:], uint32(v.Header[2]))

		_, e = t.conn.Write(headerBuf)
		if e != nil {
			return
		}
		_, e = t.conn.Write(v.Body)
		if e != nil {
			return
		}
	}
}
