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
	"errors"
	"fmt"
	"sync"
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
	serverAddr     string //host:port
	mutex          sync.Mutex
}

func newSsdbAsynClient() (*SsdbAsynClient, error) {
	asynClient := new(SsdbAsynClient)
	if asynClient == nil {
		return nil, errors.New("newSsdbAsynclient: out of memory to allocate memory")
	}

	client := new(Client)
	if client == nil {
		return nil, errors.New("newSsdbAsynclient: out of memory to allocate memory")
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
			fault := errors.New("proccess asynchronous request " + err.Error())
			asynClient.faults <- fault
		}
	}()

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
			fault := errors.New("process asynchronous response " + err.Error())
			asynClient.faults <- fault
		}
	}()

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
			err = errors.New("SsdbAsyn: failed to start up: " + re.(error).Error())
		}
	}()

	go procAsynRequests(asynClient)
	go procAsynResponses(asynClient)

	asynClient.respsCntl <- start
	asynClient.reqsCntl <- start

	return nil
}

//connect SSDB server
//Note:
//it just support IPv4
//if sec is greater than zero,the function will connect SSDB-server with a timeout.
func SsdbAsynConnect(ip string, port int, sec int) (*SsdbAsynClient, error) {
	var client *Client

	asynClient, err := newSsdbAsynClient()
	if err != nil {
		return nil, err
	}

	client, err = Connect(ip, port, sec)
	if err == nil {
		asynClient.serverAddr = fmt.Sprintf("%s:%d", ip, port)
		asynClient.client = client

		err = asynClient.startup()
	}

	if err != nil {
		asynClient = nil
	}

	return asynClient, err
}

//ssdb asynchronous disconect:
//close request channel
func (asynClient *SsdbAsynClient) SsdbAsynDisconnect() {
	go func() {
		defer func() {
			if re := recover(); re != nil {
				err := re.(error)
				fault := errors.New("Ssdb Asynchronous disconnect " + err.Error())
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

func (asynClient *SsdbAsynClient) SsdbAsynSetTimeout(sec int) {
	asynClient.client.SetTimeout(sec)
}

func (asynClient *SsdbAsynClient) Do(callback responseCallback, args ...interface{}) error {
	select {
	case _, ok := <-asynClient.shutdown:
		if ok { //the channel hasn't been closed,so we close it.
			close(asynClient.requestsQueue) //sender close the channel
		}
		return errors.New("connection to SSDB server " + asynClient.serverAddr + " has been closed")
	case <-asynClient.faults:
		return errors.New("something bad happened,so we have no choice but to stop.")
	default:
	}

	req := new(SsdbAsynRequest)
	req.callback = callback

	var buf *bytes.Buffer = &req.packet
	err := encode(buf, args)
	if err != nil {
		return err
	}

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
