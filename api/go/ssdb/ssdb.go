package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	sock     *net.TCPConn
	recv_buf bytes.Buffer
}

func Connect(ip string, port int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	var c Client
	c.sock = sock
	return &c, nil
}

func ConnectWithTimeout(ip string, port int, sec time.Duration) (*Client, error) {
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

	client := new(Client)
	client.sock = tcpConn

	return client, err
}

func (c *Client) Do(args ...interface{}) ([]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.recv()
	return resp, err
}

func (c *Client) Set(key string, val string) (interface{}, error) {
	resp, err := c.Do("set", key, val)
	if err != nil {
		return nil, err
	}
	if len(resp) == 1 && resp[0] == "ok" {
		return true, nil
	}
	return nil, fmt.Errorf("bad response")
}

// TODO: Will somebody write addition semantic methods?
func (c *Client) Get(key string) (interface{}, error) {
	resp, err := c.Do("get", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1], nil
	}
	if resp[0] == "not_found" {
		return nil, nil
	}
	return nil, fmt.Errorf("bad response")
}

func (c *Client) Del(key string) (interface{}, error) {
	resp, err := c.Do("del", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 1 && resp[0] == "ok" {
		return true, nil
	}
	return nil, fmt.Errorf("bad response")
}

func encode(res *bytes.Buffer, args []interface{}) error {
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
		res.WriteString(fmt.Sprintf("%d", len(s)))
		res.WriteByte('\n')
		res.WriteString(s)
		res.WriteByte('\n')
	}
	res.WriteByte('\n')

	return nil
}

func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	err := encode(&buf, args)
	if err != nil {
		return err
	}

	_, err = c.sock.Write(buf.Bytes())

	return err
}

func (c *Client) recv() ([]string, error) {
	var tmp [8192]byte
	for {
		//the c.recv_buf maybe have some data
		//becase we just process a packet once a time.
		if c.recv_buf.Len() > 0 {
			resp := c.parse()
			if resp == nil || len(resp) > 0 {
				return resp, nil
			}
		}

		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}

		c.recv_buf.Write(tmp[0:n])
	}
}

func (c *Client) seekNewLine() int {
	s := c.recv_buf.String()
	idx := strings.Index(s, "\n\n")

	return idx
}

func (c *Client) parse() []string {
	var idx, offset int

	idx = 0
	offset = 0
	resp := []string{}

	end := c.seekNewLine() //find the end of a complete packet
	if end == -1 {
		//the packet is not a complete reply packet yet.
		return resp
	}

	buf := c.recv_buf.Bytes()[0 : end+2] //skip '\n\n'
	c.recv_buf.Next(end + 2)             //go to next packet

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1 //skip the charactor '\n'
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= len(buf) {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	return []string{}
}

// Close The Client Connection
func (c *Client) Close() error {
	return c.sock.Close()
}

// set read and write timeout
func (c *Client) SetDeadline(sec time.Duration) error {
	deadline := time.Now().Add(sec * time.Second)

	return c.sock.SetDeadline(deadline)
}
