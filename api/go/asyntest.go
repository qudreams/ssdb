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

	//set read and write timeout
	client.SsdbAsynSetTimeout(time.Duration(2))

	client.Do(replyCallback, "set", "key1", "20")
	client.Do(replyCallback, "get", "key1")

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
