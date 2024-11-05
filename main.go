package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", ":6379")
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

	for {
		buf := make([]byte, 1024)

		// 从 redis 客户端读取指令
		_, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				break
			}
			fmt.Println("error from reading client: ", err.Error())
			os.Exit(1)
		}
		_, err = conn.Write([]byte("+OK\r\n"))
		if err != nil {
			fmt.Println("error: ", err.Error())
			return
		}
	}
}
