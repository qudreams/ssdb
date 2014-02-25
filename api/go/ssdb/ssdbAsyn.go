/*
 *ssdb asynchronous API : Copyright to qudreams(2014)
 *All rights reserved.
 *Note:
 *It depend on ssdb synchronous client.
 *You can find more detailed documention about SSDB protocol at
 * http://www.ideawu.com/ssdb
 */

package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"
)

type CntlCode byte

const (
	_ CntlCode = iota
	start
	stop
)

type StatusCode byte

const (
	_ StatusCode = iota
	OK
	NOT_FOUND
	CLIENT_ERR
	FAIL
	ERROR
	UNKNOWN
)

const (
	DefaultReqChanCap   = 4096
	DefaultRspChanCap   = 4096
	DefaultFaultChanCap = 4096
)

type SsdbAsynReply struct {
	Err    error
	Status StatusCode
	Reply  []string
}

type responseCallback func(*SsdbAsynReply, *SsdbAsynClient)

type SsdbAsynRequest struct {
	packet   bytes.Buffer
	callback responseCallback
}

type SsdbAsynClient struct {
	client         *Client
	requestsQueue  chan *SsdbAsynRequest
	responsesQueue chan *SsdbAsynRequest
	faults         chan error
	reqsCntl       chan CntlCode
	respsCntl      chan CntlCode
	isShutdown     bool
	shutdown       chan bool
	mutex          sync.Mutex
}

func newSsdbAsynClient() (*SsdbAsynClient, error) {
	asynClient := new(SsdbAsynClient)
	if asynClient == nil {
		return nil, fmt.Errorf("newSsdbAsynclient: out of memory to allocate memory")
	}

	client := new(Client)
	if client == nil {
		return nil, fmt.Errorf("newSsdbAsynclient: out of memory to allocate memory")
	}

	asynClient.client = client
	asynClient.requestsQueue = make(chan *SsdbAsynRequest, DefaultReqChanCap)
	asynClient.responsesQueue = make(chan *SsdbAsynRequest, DefaultRspChanCap)
	asynClient.faults = make(chan error, DefaultFaultChanCap)
	asynClient.reqsCntl = make(chan CntlCode, 1)
	asynClient.respsCntl = make(chan CntlCode, 1)
	asynClient.shutdown = make(chan bool, 1)
	asynClient.isShutdown = false

	return asynClient, nil
}

func procAsynRequests(asynClient *SsdbAsynClient) {
	defer func() {
		if re := recover(); re != nil {
			err := re.(error)
			fault := fmt.Errorf("proccess asynchronous request %s", err.Error())
			asynClient.faults <- fault
		}
	}()

	select {
	case cntl := <-asynClient.reqsCntl:
		if cntl == stop {
			close(asynClient.responsesQueue)
			return
		}
	}

	for {
		select {
		case cntl := <-asynClient.reqsCntl:
			if cntl == stop {
				close(asynClient.responsesQueue) //Note: sender close the channel
				return
			}
		case req, ok := <-asynClient.requestsQueue:
			if ok {
				err := asynClient.sendRequest(req)
				if err != nil {
					panic(err)
				}
				asynClient.responsesQueue <- req
			}
		}
	}
}

func procAsynResponses(asynClient *SsdbAsynClient) {
	defer func() {
		if re := recover(); re != nil {
			err := re.(error)
			fault := fmt.Errorf("process asynchronous response %s", err.Error())
			asynClient.faults <- fault
		}
	}()

	select {
	case cntl := <-asynClient.respsCntl:
		if cntl == stop {
			return
		}
	}

	for {
		select {
		case cntl := <-asynClient.respsCntl:
			if cntl == stop {
				return
			}
		case rsp, ok := <-asynClient.responsesQueue:
			if ok {
				rep := asynClient.recvResponse()
				rsp.callback(rep, asynClient)
			}
		}
	}
}

func (asynClient *SsdbAsynClient) startup() (err error) {
	defer func() {
		if re := recover(); re != nil {
			err = fmt.Errorf("SsdbAsyn: failed to start up: %v", re.(error))
		}
	}()

	go procAsynRequests(asynClient)
	asynClient.reqsCntl <- start
	go procAsynResponses(asynClient)
	asynClient.respsCntl <- start

	return nil
}

//connect SSDB server without timeout
//Note: it just support IPv4
func SsdbAsynConnect(ip string, port int) (*SsdbAsynClient, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, fmt.Errorf("SsdbAsyn: failed to parse server address %s", err.Error())
	}

	asynClient, err := newSsdbAsynClient()
	if err != nil {
		return nil, err
	}

	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("SsdbAsyn: %s", err.Error())
	}

	client := asynClient.client
	client.sock = sock

	err = asynClient.startup()

	return asynClient, err
}

func SsdbAsynConnectWithTimeout(ip string, port int, sec time.Duration) (*SsdbAsynClient, error) {
	asynClient, err := newSsdbAsynClient()
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", ip, port)
	conn, err := net.DialTimeout("tcp", addr, sec*time.Second)
	if err != nil {
		return nil, fmt.Errorf("SsdbAsyn: %s", err.Error())
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("SsdbAsyn: type assert failed from net.Conn to net.TCPConn")
	}

	client := asynClient.client
	client.sock = tcpConn

	err = asynClient.startup()

	return asynClient, err
}

//ssdb asynchronous disconect:
//close request channel
func (asynClient *SsdbAsynClient) SsdbAsynDisconnect() {
	go func() {
		defer func() {
			if re := recover(); re != nil {
				err := re.(error)
				fault := fmt.Errorf("Ssdb Asynchronous disconnect %s", err.Error())
				asynClient.faults <- fault
			}
		}()

		asynClient.mutex.Lock()
		if asynClient.isShutdown == true {
			asynClient.mutex.Unlock()
			return
		}
		asynClient.isShutdown = true
		asynClient.mutex.Unlock()

		asynClient.reqsCntl <- stop
		asynClient.respsCntl <- stop
		asynClient.shutdown <- true

		/*Note:
		 *sender close the channel
		 */
		close(asynClient.reqsCntl)
		close(asynClient.respsCntl)
		close(asynClient.shutdown)

		asynClient.client.Close()
		asynClient.client = nil
	}()
}

func (asynClient *SsdbAsynClient) SsdbAsynSetTimeout(sec time.Duration) {
	asynClient.client.SetDeadline(sec)
}

func (asynClient *SsdbAsynClient) Do(callback responseCallback, args ...interface{}) error {
	select {
	case _, ok := <-asynClient.shutdown:
		if ok { //the channel hasn't been closed,so we close it.
			close(asynClient.requestsQueue) //sender close the channel
		}
		return fmt.Errorf("connection to SSDB has been closed")
	case <-asynClient.faults:
		return fmt.Errorf("something bad happened,so we have no choice but to stop.")
	default:
	}

	req := new(SsdbAsynRequest)
	req.callback = callback

	var buf *bytes.Buffer = &req.packet
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')

	asynClient.requestsQueue <- req

	return nil
}

func (asynClient *SsdbAsynClient) sendRequest(r *SsdbAsynRequest) error {
	client := asynClient.client

	_, err := client.sock.Write(r.packet.Bytes())

	return err
}

func (asyncClient *SsdbAsynClient) recvResponse() *SsdbAsynReply {
	client := asyncClient.client
	rep := new(SsdbAsynReply)

	rep.Reply, rep.Err = client.recv()
	if rep.Err == nil {
		//we got a reply
		switch rep.Reply[0] {
		case "ok":
			rep.Status = OK
		case "not_found":
			rep.Status = NOT_FOUND
		case "client_error":
			rep.Status = CLIENT_ERR
		case "fail":
			rep.Status = FAIL
		case "error":
			rep.Status = ERROR
		default:
			rep.Status = UNKNOWN
		}

		//skip status flag of the reply.
		if len(rep.Reply) > 1 {
			rep.Reply = rep.Reply[1:]
		} else {
			rep.Reply = []string{}
		}
	}

	return rep
}

func (asynClient *SsdbAsynClient) IsShutdown() bool {
	defer asynClient.mutex.Unlock()

	asynClient.mutex.Lock()
	b := asynClient.isShutdown

	return b
}
