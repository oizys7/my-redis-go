package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

type Server struct {
	l                net.Listener
	conns            []*ServerConnection
	keysExpiryTicker *time.Ticker
}
type ServerConnection struct {
	con net.Conn
}

func (s *Server) Start() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	fmt.Println("Listening on port: " + *port)

	// TCP 连接异常处理
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	s.l = l
	// 每秒都触发一次，检查过期的 key
	s.keysExpiryTicker = time.NewTicker(1 * time.Second)
	go s.triggerActiveExpiryCheck()
	for {
		con, err := s.l.Accept()
		// 端口监听异常处理
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		serverCon := &ServerConnection{
			con: con,
		}
		s.conns = append(s.conns, serverCon)
		go serverCon.handler()
	}
}
func (s *Server) Close() {
	_ = s.l.Close()
}

func (s *Server) triggerActiveExpiryCheck() {
	defer s.keysExpiryTicker.Stop()
	for {
		<-s.keysExpiryTicker.C
		for key, val := range SETs {
			if (val.ExpiryInMS.Before(time.Now()) && val.ExpiryInMS != time.Time{}) {
				fmt.Printf("deleting key :%v", key)
				delete(SETs, key)
			}
		}

		//for k1, entryMap := range HSETs {
		//	for k2, _ := range entryMap {
		//		if (entryMap[k2].ExpiryInMS.Before(time.Now()) && entryMap[k2].ExpiryInMS != time.Time{}) {
		//			fmt.Printf("deleting key :%v", k2)
		//			delete(entryMap, k2)
		//		}
		//	}
		//	if len(entryMap) == 0 {
		//		delete(HSETs, k1)
		//	}
		//}
	}
}

func (sc *ServerConnection) handler() {
	for {
		conn := sc.con
		resp := NewResp(conn)
		value, err := resp.Read()

		if err != nil {
			if err != io.EOF {
				fmt.Println("error from reading client: ", err.Error())
			}
			continue
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		fmt.Println("从客户端接收到的数据：")
		fmt.Println(value)
		writer := NewWriter(conn)

		// 处理命令
		handle, ok := Handlers[command]
		if !ok {
			err := writer.Write(Value{typ: ERROR, str: "Invalid command: " + command})
			if err != nil {
				fmt.Println("error: ", err.Error())
				return
			}
			continue
		}

		// 向 redis Client 回写数据
		err = writer.Write(handle(args))
		if err != nil {
			fmt.Println("error: ", err.Error())
			return
		}
	}
}
