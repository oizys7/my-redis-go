package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
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
		resp := NewResp(conn)
		value, err := resp.Read()

		if err != nil {
			if err != io.EOF {
				break
			}
			fmt.Println("error from reading client: ", err.Error())
			os.Exit(1)
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

		fmt.Println(value)
		writer := NewWriter(conn)

		// 处理命令
		handle, ok := Handlers[command]
		if !ok {
			//fmt.Println("Invalid command: ", command)
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
