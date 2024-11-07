package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// 对应 RESP 的类型
const (
	CommandString  = '+'
	CommandError   = '-'
	CommandInteger = ':'
	CommandBulk    = '$'
	CommandArray   = '*'
)

// 支持的类型标识
const (
	NULL    = "NULL"
	STRING  = "STRING"
	ERROR   = "ERROR"
	INTEGER = "INTEGER"
	BULK    = "BULK"
	ARRAY   = "ARRAY"
)

// Value redis 命令 set admin ahmed
// Value resp 格式 *3\r\n$3\r\nset\r\n$5\r\nadmin\r\n$5\r\nahmed
type Value struct {
	typ   string
	str   string  // 保存从简单字符串接收到的字符串值
	num   int     // 保存接收到到整数值
	bulk  string  // 保存从批量字符串接收到的字符串值
	array []Value // 保存从数组接收到的所有值
}

type Writer struct {
	writer io.Writer
}

type Resp struct {
	reader *bufio.Reader
}

// NewResp 传递 main 中创建的连接中的缓冲区
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// 在这个函数中，我们每次读取一个字节，直到到达 '\r'，表示行尾。然后，我们返回没有最后 2 个字节（即 '\r\n'）的行数以及行中的字节数。
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

// 将第 n 行转化位整数
func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	// base 表示数的进制， bitSize 表示返回的整数类型是64位的
	// 将 line 转换为64位有符号整数，并捕获可能发生的错误
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	// int(i64)：主要是考虑到 int 在 32/64 位操作系统中的长度不一样
	return int(i64), n, nil
}

// -------------------------------- 解析和反序列化 --------------------------------
// -------------------------- 把 RESP 转化为 Value 结构体 -------------------------
// 调用执行解析的具体函数
func (r *Resp) Read() (Value, error) {
	// 通过读取第一个字节来确定要解析的 RESP 类型
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case CommandArray:
		return r.readArray()
	case CommandBulk:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// 解析一个数组
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	// read length of array
	arrayLen, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// foreach line, parse and read the value
	v.array = make([]Value, 0)
	for i := 0; i < arrayLen; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		// append parsed value to array
		v.array = append(v.array, val)
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	bulkLen, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, bulkLen)
	_, err = r.reader.Read(bulk)
	if err != nil {
		return Value{}, err
	}
	v.bulk = string(bulk)

	// Read the trailing CRLF
	_, _, err = r.readLine()
	if err != nil {
		return Value{}, err
	}

	return v, nil
}

// -------------------------- 把 Value 结构体转化为 RESP -------------------------

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	logger.Debug("服务端返回的数据：")
	logger.Debug(string(bytes))

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (v Value) Marshal() []byte {
	switch v.typ {
	case ARRAY:
		return v.marshalArray()
	case BULK:
		return v.marshalBulk()
	case STRING:
		return v.marshalString()
	case INTEGER:
		return v.marshalInteger()
	case NULL:
		return v.marshallNull()
	case ERROR:
		return v.marshallError()
	default:
		return []byte{}
	}
}

func (v Value) marshalArray() []byte {
	var bytes []byte
	bytes = append(bytes, CommandArray)
	bytes = append(bytes, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len(v.array); i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}
	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, CommandBulk)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, CommandString)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}
func (v Value) marshalInteger() []byte {
	var bytes []byte
	bytes = append(bytes, CommandInteger)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, CommandError)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}
