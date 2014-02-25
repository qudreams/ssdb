package main

import (
	"./ssdb"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	client, err := ssdb.SsdbAsynConnectWithTimeout("127.0.0.1", 8888, time.Duration(5))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}

	// set read and write timeout
	client.SsdbAsynSetTimeout(time.Duration(2))
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		go func(k string, v int) {
			client.Do(replyCallback, "set", k, v)
			client.Do(replyCallback, "get", k)
			client.Do(replyCallback, "del", k)
		}(key, i)
	}

	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("zset%d", i)
		value := fmt.Sprintf("zv%d", i)
		go func(nm string, v string, sc int) {
			client.Do(replyCallback, "zset", nm, v, sc)
			client.Do(replyCallback, "zscan", nm, "", "", "", 100)
			client.Do(replyCallback, "zclear", nm)
		}(name, value, i)
	}

	time.Sleep(4 * time.Second)
}

func replyCallback(reply *ssdb.SsdbAsynReply, asynClient *ssdb.SsdbAsynClient) {
	if reply.Err != nil {
		log.Println("error: " + reply.Err.Error())
		asynClient.SsdbAsynDisconnect()
		return
	}

	if reply.Status == ssdb.OK {
		for i := 0; i < len(reply.Reply); i++ {
			fmt.Println(reply.Reply[i])
		}
	}
}
