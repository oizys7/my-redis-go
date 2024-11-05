package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	var port = "6379"
	l, err := net.Listen("tcp", ":"+port)
	fmt.Println("Listening on port: " + port)
	// TCP 连接异常处理
	if err != nil {
		fmt.Println(err)
		return
	}

	conn, err := l.Accept()
	// 端口监听异常处理
	if err != nil {
		fmt.Println(err)
	}

	// 请求完成关闭 TCP 连接
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	for {
		//buf := make([]byte, 1024)
		// 从 redis 客户端读取指令
		//_, err := conn.Read(buf)

		resp := NewResp(conn)
		value, err := resp.Read()

		if err != nil {
			if err != io.EOF {
				break
			}
			fmt.Println("error from reading client: ", err.Error())
			os.Exit(1)
		}
		fmt.Println(value)
		_, err = conn.Write([]byte("+OK\r\n"))
		if err != nil {
			fmt.Println("error: ", err.Error())
			return
		}
	}
}
