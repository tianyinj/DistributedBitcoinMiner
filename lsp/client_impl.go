// Contains the implementation of a LSP client.
package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"container/list"
	"encoding/json"
	"time"
)

type client struct {
	// TODO: implement this!
	conn              *lspnet.UDPConn
	success_conn      chan bool
	success_conn_bool bool
	recv_data         bool
	connID            int

	write_buf_channel chan []byte
	write_channel     chan Message

	read_buf_channel chan Message
	read_channel     chan []byte

	epoch_channel chan int

	write_seqNum int

	window_size       int
	not_ack_msg       map[int]Message
	out_of_window_msg *list.List

	epochMillis  int
	epochLimit   int
	epochCounter int

	unread_msg    map[int]Message
	expect_seqNum int

	start_to_close chan int
	start_to_close_flag   bool
	success_close  chan bool

	shutDown chan int

}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").

func NewClient(hostport string, params *Params) (Client, error) {
	if addr, err := lspnet.ResolveUDPAddr("udp", hostport); err != nil {
		return nil, err
	} else if conn, err := lspnet.DialUDP("udp", nil, addr); err != nil {
		return nil, err
	} else {
		c := new(client)
		c.conn = conn
		c.success_conn = make(chan bool, 1)
		c.success_conn_bool = false
		c.recv_data = false

		c.write_buf_channel = make(chan []byte, 100000)
		c.write_channel = make(chan Message, 100000)
		c.read_channel = make(chan []byte, 100000)
		c.read_buf_channel = make(chan Message, 1000000)
		c.epoch_channel = make(chan int, 10)


		c.write_seqNum = 1

		c.window_size = params.WindowSize
		c.not_ack_msg = make(map[int]Message)
		c.out_of_window_msg = list.New()

		c.epochMillis = params.EpochMillis
		c.epochLimit = params.EpochLimit
		c.epochCounter = 0

		c.unread_msg = make(map[int]Message)
		c.expect_seqNum = 1

		c.start_to_close = make(chan int, 1)
		c.start_to_close_flag = false
		c.success_close = make(chan bool, 1)

		c.shutDown = make(chan int)

		c.connID = 0

		msg := NewConnect()
		c.write_channel <- *msg


		go c.read_from_server()
		go c.write_to_server()
		go c.runClient()
		go c.epochEvent()

		//blocks until a connection with the server has been made
		if <-c.success_conn {
			c.success_conn_bool = true
			fmt.Printf("client %v success_conn \n", c.connID)
			return c, nil
		} else {
			return nil, errors.New("Failed to establish connection")
		}


	}

}

func (c *client) epochEvent() {
	tick := time.Tick(time.Duration(c.epochMillis) * time.Millisecond)
	for {

		select {
		case <- c.shutDown:
			return
		case <-tick:
			c.epoch_channel <- 1
		}
	}
}

func (c *client) read_from_server() {
	for {
		select{
		case <- c.shutDown:
			return

		default:

			var msg Message
			var buf [2000]byte
			n, err := c.conn.Read(buf[0:])

			if err == nil {
				json.Unmarshal(buf[0:n], &msg)
				c.read_buf_channel <- msg
			}
			
			
		}
	}	
}

func (c *client) write_to_server() {
	for {
		select {
		case <- c.shutDown:
			return

		case msg := <-c.write_channel:
			buf, _ := json.Marshal(&msg)
			c.conn.Write(buf)

		}
	}
}

func (c *client) runClient() {
	for {

		select {
		case <-c.start_to_close:
			c.start_to_close_flag = true

		case <-c.epoch_channel:

			c.epochCounter += 1

			if (!c.success_conn_bool && (c.epochCounter >= c.epochLimit)){
				c.success_conn <- false

			}

			if len(c.not_ack_msg) == 0 && c.out_of_window_msg.Len() == 0 && c.start_to_close_flag {
				c.success_close <- true
				close(c.shutDown)
				return
			} else if c.epochCounter >= c.epochLimit {
				fmt.Printf("client lost due to epoch\n")
				if c.start_to_close_flag {
					c.success_close <- false
				} 
				close(c.shutDown)
				fmt.Printf("Client %v is returned\n", c.connID)
				return
			} else {
				if c.connID == 0 {
					msg := NewConnect()
					c.write_channel <- *msg
				} else if !c.recv_data && len(c.not_ack_msg) == 0 {
					msg := NewAck(c.connID, 0)
					c.write_channel <- *msg
				} else {
					for _, msg := range c.not_ack_msg {
						c.write_channel <- msg
					}
				}
			}

		case buf := <-c.write_buf_channel:


			msg := NewData(c.connID, c.write_seqNum, len(buf), buf)

			if len(c.not_ack_msg) < c.window_size {
				min_not_ack_seqNum := msg.SeqNum + 1
				for k, _ := range c.not_ack_msg {
					if k < min_not_ack_seqNum {
						min_not_ack_seqNum = k
					}
				}
				if msg.SeqNum < min_not_ack_seqNum+c.window_size {
					c.write_channel <- *msg
					c.not_ack_msg[msg.SeqNum] = *msg
				} else {
					c.out_of_window_msg.PushBack(*msg)
				}
			} else {
				c.out_of_window_msg.PushBack(*msg)
			}
			c.write_seqNum += 1

		case msg := <-c.read_buf_channel:



			c.epochCounter = 0

			if msg.Type == MsgAck {
				if msg.SeqNum == 0 && c.connID == 0{
					c.connID = msg.ConnID
					c.success_conn <- true
					continue
				}

				delete(c.not_ack_msg, msg.SeqNum)

				min_not_ack_seqNum := msg.SeqNum + 1
				for k, _ := range c.not_ack_msg {
					if k < min_not_ack_seqNum {
						min_not_ack_seqNum = k
					}
				}

				for m := c.out_of_window_msg.Front(); len(c.not_ack_msg) < c.window_size && m != nil; m = m.Next() {
					if m.Value.(Message).SeqNum < min_not_ack_seqNum+c.window_size {
						c.out_of_window_msg.Remove(m)
						c.not_ack_msg[m.Value.(Message).SeqNum] = m.Value.(Message)
						c.write_channel <- m.Value.(Message)

					}
				}

			} else if msg.Type == MsgData {
				if len(msg.Payload) < msg.Size {
					break
				}

				if !c.recv_data {
					c.recv_data = true
				}
				ack := NewAck(c.connID, msg.SeqNum)
				c.write_channel <- *ack

				if msg.SeqNum == c.expect_seqNum {
					c.read_channel <- msg.Payload[0:msg.Size]
					c.expect_seqNum += 1
				} else {
					c.unread_msg[msg.SeqNum] = msg
				}

				for {
					if m, ok := c.unread_msg[c.expect_seqNum]; !ok {
						break
					} else {
						c.read_channel <- m.Payload
						delete(c.unread_msg, c.expect_seqNum)
						c.expect_seqNum += 1
					}
				}

			}
		}

	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {

	if c.start_to_close_flag {
		return nil, errors.New("Connection is closed")
	}
	
	select{
	case msg := <-c.read_channel:
		return msg, nil
	case <- c.shutDown:
		return nil, errors.New("Connection is lost")
	}
	
}

func (c *client) Write(payload []byte) error {

	if c.start_to_close_flag {
		return errors.New("Connection is closed")
	}
	c.write_buf_channel <- payload
	return nil
}

func (c *client) Close() error {
	c.start_to_close <- 1
	select {
	case flag := <-c.success_close:
		if flag {
			return nil
		} else {
			return errors.New("Connection is lost")
		}
	}
	return nil

}

