/*
 *asyn_test.go: 2014-04-08 created by qudreams.
 *testing to SSDB asynchronous API.
 */

package ssdb

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func Test_Connect(t *testing.T) {
	conn, err := SsdbAsynConnect("127.0.0.1", 8888, 0)
	if err != nil {
		t.Error(err.Error())
	} else {
		conn.SsdbAsynDisconnect()
		t.Log("connect success to server: 127.0.0.1:8888")
	}
}

//testing to close connection serveral times.
func Test_ConnectClose(t *testing.T) {
	conn, err := SsdbAsynConnect("127.0.0.1", 8888, 0)
	if err != nil {
		t.Error(err.Error())
	} else {
		wg := new(sync.WaitGroup)

		wg.Add(3)

		go func() {
			conn.SsdbAsynDisconnect()
			wg.Done()
		}()
		go func() {
			conn.SsdbAsynDisconnect()
			wg.Done()
		}()

		go func() {
			err = conn.Do(replyCallback, "set", "key1", 10)
			if err != nil {
				t.Error(err.Error())
			}
			wg.Done()
		}()

		err = conn.Do(replyCallback, "set", "key1", 10)
		if err != nil {
			t.Error(err.Error())
		}

		wg.Wait()
	}
}

func Test_DoKeyValue(t *testing.T) {
	conn, err := SsdbAsynConnect("127.0.0.1", 8888, 5)
	if err != nil {
		t.Error(err.Error())
		return
	}

	defer conn.SsdbAsynDisconnect()

	err = conn.Do(replyCallback, "set", "key1", 10)
	if err != nil {
		t.Error(err.Error())
	}

	err = conn.Do(replyCallback, "get", "key1")
	if err != nil {
		t.Error(err.Error())
	}

	err = conn.Do(replyCallback, "del", "key1")
	if err != nil {
		t.Error(err.Error())
	}

	err = conn.Do(replyCallback, "setx", "keyttl", 20, 20)
	if err != nil {
		t.Error(err.Error())
	}

	err = conn.Do(replyCallback, "get", "keyttl")
	if err != nil {
		t.Error(err.Error())
	}

	err = conn.Do(replyCallback, "list", "", "", 20)
	if err != nil {
		t.Error(err.Error())
	}

	err = conn.Do(replyCallback, "keys", "", "", 20)
	if err != nil {
		t.Error(err.Error())
	}

	time.Sleep(time.Duration(5))
}

func Test_ConnectWithTimeout(t *testing.T) {
	conn, err := SsdbAsynConnect("182.169.1.112", 8888, 5)
	if err != nil {
		t.Error(err.Error())
	} else {
		conn.SsdbAsynDisconnect()
		t.Log("connect success to server: 192.169.1.112:8888")
	}
}

func replyCallback(reply *SsdbAsynReply, asynClient *SsdbAsynClient) {
	if reply.Err != nil {
		log.Println("error: " + reply.Err.Error())
		asynClient.SsdbAsynDisconnect()
		return
	}

	if reply.Status == OK {
		for _, val := range reply.Reply {
			fmt.Println(val)
		}
	}
}
