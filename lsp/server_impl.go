package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"container/list"
	"encoding/json"
	"strconv"
	"time"
)

type server struct {
	conn            *lspnet.UDPConn //Port the server is listening on

	new_conn_chan chan *lspnet.UDPAddr
	next_ConnID int

	client_db map[int]*clientData

	read_buffer     chan Message //Buffer channel for incoming msg
	next_read 		chan Message //Fan out incoming 

	write_buffer    chan Message //Buffer channel for all outgoing msg
	write_reply     chan bool

	//Client handling channels
	offlineClient chan int //IDs of offline clients

	//Server status flags
	close_server chan int
	close_server_flag bool   //Flagged immediately when server is shut down

	close_conn chan int
	close_conn_reply chan bool
	
	success_close chan bool
	success_close_queue chan bool

	shutDown chan int //Signal Channel for recycling goroutines

	window_size int
	epochLimit int
	epochMillis int
}

type clientData struct {

	conn      *lspnet.UDPAddr 
	connID int

	SeqNum int //Next sequence number to use
	write_seqNum  int

	write_buffer   chan Message //Buffer channel outgoing msg to the client

	recv_data     bool
	epoch_channel chan int

	not_ack_msg       map[int]Message
	out_of_window_msg *list.List

	epochCounter int

	unread_msg    map[int]Message
	expect_seqNum int

	goingToClose      bool
	write_channel     chan Message
	read_buffer  chan Message

	close_conn chan int
	close_conn_reply chan bool

	shutDown chan int
}


func NewServer(port int, params *Params) (Server, error) {
	if addr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("localhost", strconv.Itoa(port))); err != nil {
		return nil, err
	} else if listener, err := lspnet.ListenUDP("udp", addr); err != nil {
		return nil, err
	} else {
		s := new(server)
		s.conn = listener

		s.new_conn_chan = make(chan *lspnet.UDPAddr, 100000)
		s.next_ConnID = 1

		s.client_db = make(map[int]*clientData)

		s.read_buffer = make(chan Message, 100000)
		s.next_read = make(chan Message, 100000)

		s.write_buffer = make(chan Message, 100000)
		s.write_reply = make(chan bool)

		s.offlineClient = make(chan int, 10000)

		s.close_conn = make(chan int, 10000)
		s.close_conn_reply = make(chan bool, 10000)

		s.close_server = make(chan int)
		s.close_server_flag = false

		s.success_close_queue = make(chan bool, 10000)
		s.success_close = make(chan bool)

		s.window_size = params.WindowSize
		s.epochMillis = params.EpochMillis
		s.epochLimit = params.EpochLimit

		s.shutDown = make(chan int)

		go s.read_from_connection()
		go s.serverEventHandler()

		return s, nil
	}
}

func (s *server) Read() (int, []byte, error) {
	if s.close_server_flag {
	 	return 0, nil, errors.New("server is shut down")
	}
	select {
	case msg := <-s.next_read:
		return msg.ConnID, msg.Payload[0:msg.Size], nil
	case c_id := <-s.offlineClient: //connection is lost OR closed
		return c_id, nil, errors.New("Read error")
	}
}

func (s *server) Write(ConnID int, payload []byte) error {
	if s.close_server_flag {
	 	return errors.New("server is shut down")
	}
	msg := NewData(ConnID, -1, len(payload), payload)
	s.write_buffer <- *msg

	if !<-s.write_reply {
		return errors.New("Client doesn't exist")
	}
	
	return nil
	
}

func (s *server) CloseConn(ConnID int) error {
	if s.close_server_flag {
		return errors.New("server is shut down")
	}
	s.close_conn <- ConnID
	if !(<-s.close_conn_reply) {
		return errors.New("Client doesn't exist")
	} else {
		s.offlineClient <- ConnID
		return nil
	}
}

func (s *server) Close() error {

	s.close_server <- 1	

	flag := true
	for i := 0; i < len(s.client_db); i++ {
		if !<-s.success_close_queue {
			flag = false
		}	
	}
	if flag{
		close(s.shutDown)
		fmt.Printf("Server closed normally\n")
		return nil
	} else {
		close(s.shutDown)
		fmt.Printf("Server closed\n")
		return errors.New("Not all client are closed\n")
	}
}

// Return iff reached epoch limit or closed
func (s *server) clientEventHandler(c *clientData) {
	for {
		select {
		case msg := <- c.write_buffer:
			if len(c.not_ack_msg) < s.window_size {
				min_unack_seqNum := msg.SeqNum
				for k, _ := range c.not_ack_msg {
					if k < min_unack_seqNum {
						min_unack_seqNum = k
					}
				}
				if msg.SeqNum < min_unack_seqNum + s.window_size {

					c.write_channel <- msg
					c.not_ack_msg[msg.SeqNum] = msg

				} else {
					c.out_of_window_msg.PushBack(msg)
				}
			} else {
				c.out_of_window_msg.PushBack(msg)
			}
		case <- c.close_conn:
			c.goingToClose = true


		case <- c.epoch_channel:
			if len(c.not_ack_msg) == 0 && c.out_of_window_msg.Len() == 0{
				if c.goingToClose {
					if s.close_server_flag{
						s.success_close_queue <- true
					} 
					close(c.shutDown)
					fmt.Printf("client shutted down\n")
					return
				}
			}
			c.epochCounter += 1
			if c.epochCounter >= s.epochLimit {
				s.offlineClient <- c.connID
				if s.close_server_flag{
					s.success_close_queue <- false
				}

				
				close(c.shutDown)
				fmt.Printf("client shutted down due to time out\n")
				return
			} else {
				if !c.recv_data && len(c.not_ack_msg) == 0 {
					response := NewAck(c.connID, 0)
					c.write_channel <- *response
				} else {
					for _, msg := range c.not_ack_msg {
						
						c.write_channel <- msg
					}
				}
			}
		case msg := <-c.read_buffer:
			c.epochCounter = 0
			if msg.Type == MsgAck {

				if msg.SeqNum == 0 {
					for _, r_msg := range c.not_ack_msg {
						c.write_channel <- r_msg
					}
				}
				delete(c.not_ack_msg, msg.SeqNum)
				min_not_ack_seqNum := msg.SeqNum + 1
				for k, _ := range c.not_ack_msg {
					if k < min_not_ack_seqNum {
						min_not_ack_seqNum = k
					}
				}

				for m := c.out_of_window_msg.Front(); len(c.not_ack_msg) < s.window_size && m != nil; m = m.Next() {
					if m.Value.(Message).SeqNum < min_not_ack_seqNum+s.window_size {
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

				response := NewAck(c.connID, msg.SeqNum)
				c.write_channel <- *response

				if msg.SeqNum == c.expect_seqNum {
					s.next_read <- msg
					c.expect_seqNum += 1
				} else {
					c.unread_msg[msg.SeqNum] = msg
				}

				for {

					if m, ok := c.unread_msg[c.expect_seqNum]; !ok {
						break
					} else {

						s.next_read <- m
						delete(c.unread_msg, c.expect_seqNum)
						c.expect_seqNum += 1
					}
				}
			}
		}
	}
}

// Recycled on Close()
func (s *server) serverEventHandler() {
	for {
		select {	
		case <- s.shutDown :
			return	
		case msg := <- s.write_buffer:
			c, ok := s.client_db[msg.ConnID]
			s.write_reply <- ok
			if ok {
				msg.SeqNum = c.write_seqNum
				c.write_seqNum += 1
				c.write_buffer <- msg
				
			}
		case addr := <- s.new_conn_chan:
			c := new(clientData)

			c.conn = addr
			c.connID = s.next_ConnID
			c.write_seqNum = 1
			c.recv_data = false
			c.not_ack_msg = make(map[int]Message)
			c.out_of_window_msg = list.New()
			c.epochCounter = 0
			c.unread_msg = make(map[int]Message)
			c.expect_seqNum = 1
			c.epoch_channel = make(chan int, 10)

			c.goingToClose = false
			c.read_buffer = make(chan Message, 1000)
			c.write_channel = make(chan Message, 100000)
			c.write_buffer = make(chan Message, 100000)

			c.epoch_channel = make(chan int, 10)
			c.close_conn = make (chan int)
			c.close_conn_reply = make(chan bool)

			go s.clientEpochHandler(c)
			go s.clientEventHandler(c)
			go s.write_to_connection(c)

			response := NewAck(c.connID, 0)
			c.write_channel <- *response

			s.next_ConnID += 1
			s.client_db[c.connID] = c

			c.shutDown = make(chan int)
			

		case msg := <- s.read_buffer:


			c, ok := s.client_db[msg.ConnID]
			if ok {
				
				c.read_buffer <- msg
			}
		case connID := <- s.close_conn:
			c, ok := s.client_db[connID]
			s.close_conn_reply <- ok
			if ok {
				c.close_conn <- 1
			}
		case <- s.close_server:
			s.close_server_flag = true
			cnt := 0
			
			for _, c := range s.client_db {
				select{
				case <- c.shutDown:
					s.success_close_queue <- true
				default:
					c.close_conn <- 1
					cnt++
				}
			}
			
		}
	}
}

// One instance for each client
// Fires epoch and push 'tick' in to client's chanel
// Recycled by clientEventHandler
func (s *server) clientEpochHandler(c *clientData) {
	tick := time.Tick(time.Duration(s.epochMillis) * time.Millisecond)
	for {
		select {
		case <-c.shutDown:
			return
		case <-tick:
			c.epoch_channel <- 1
		}
	}
}

// Run one instance for the server
// Keeps reading from connection
// Recycled on Close()
func (s *server) read_from_connection() {
	for {
		select{
		case <- s.shutDown:
			return
		default:
			var msg Message
			var buf [2000]byte
			n, addr, err := s.conn.ReadFromUDP(buf[0:])
			if err != nil {
				return
			}
			json.Unmarshal(buf[0:n], &msg)
			if msg.Type == MsgConnect {
				if !s.close_server_flag {
					s.new_conn_chan <- addr
				}
			} else {
				s.read_buffer <- msg
			}
		}
	}
}

// One instance for each client
// Recycled by clientEventHandler
func (s *server) write_to_connection(c *clientData) {
	for {
		select {
		case <- c.shutDown:
			return
		case msg := <-c.write_channel:
			buf, _ := json.Marshal(&msg)
			s.conn.WriteToUDP(buf, c.conn)

		}
	}
}