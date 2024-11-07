package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"
)

/*
* RDB 文件格式概述
* 以下是 RDB 文件的各个部分，按顺序排列：
* 	标题部分
* 	元数据部分
* 	数据库部分
* 	文件结束部分
 */

const (
	opCodeTypeString   byte = 0   /*following byte(s) are length encoding. */
	opCodeModuleAux    byte = 247 /* Module auxiliary data. */
	opCodeIdle         byte = 248 /* LRU idle time. */
	opCodeFreq         byte = 249 /* LFU frequency. */
	opCodeAux          byte = 250 /* RDB aux field. */
	opCodeResizeDB     byte = 251 /* Hash table resize hint. */
	opCodeExpireTimeMs byte = 252 /* Expire time in milliseconds. */
	opCodeExpireTime   byte = 253 /* Old expire time in seconds. */
	opCodeSelectDB     byte = 254 /* DB number of the following keys. */
	opCodeEOF          byte = 255
)

func loadRdbFileIntoKVMemoryStore() {
	content, err := os.ReadFile(fmt.Sprintf("%s/%s", *dir, *dbFileName))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if len(content) == 0 {
		return
	}

	data, err := parseRDB(content)
	if err != nil && err.Error() != "EOF" {
		fmt.Println(err.Error())
		return
	}

	for i := 0; i < len(data); i += 2 {
		key := data[i]
		value := data[i+1]
		SETsMu.Lock()
		SETs[key] = &Entry{
			Value:       value,
			TimeCreated: time.Now(),
			ExpiryInMS:  time.Time{},
		}
		SETsMu.Unlock()
	}

	//length, err := decodeLength(reader)
	//for i := range length {
	//	key := make([]byte, line[3])
	//	value := line[5+line[3]:]
	//
	//	SETsMu.Lock()
	//	SETs[string(key)] = &Entry{
	//		Value:       string(value),
	//		TimeCreated: time.Now(),
	//		ExpiryInMS:  time.Time{},
	//	}
	//	defer SETsMu.Unlock()
	//}

}

func parseRDB(content []byte) ([]string, error) {
	var result []string
	line := parseTable(content)
	reader := bytes.NewReader(line)

	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			return result, err
		}

		switch opcode {
		case opCodeSelectDB:
			// Follwing byte(s) is the db number.
			dbNum, err := decodeLength(reader)
			if err != nil {
				return result, err
			}
			logger.Debug("DB number: " + strconv.Itoa(dbNum))
		case opCodeAux:
			// Length prefixed key and value strings follow.
			kv := [][]byte{}
			for i := 0; i < 2; i++ {
				length, err := decodeLength(reader)
				if err != nil {
					return result, err
				}
				data := make([]byte, int(length))
				if _, err = reader.Read(data); err != nil {
					return result, err
				}
				kv = append(kv, data)
			}
		case opCodeResizeDB:
			// Implement
			hashTableNum, err := decodeLength(reader)
			if err != nil {
				return result, err
			}
			_, _ = reader.ReadByte()
			logger.Debug("Hash table resize: " + strconv.Itoa(hashTableNum))
		case opCodeTypeString:
			kv := [][]byte{}
			for i := 0; i < 2; i++ {
				length, err := decodeLength(reader)
				if err != nil {
					return result, err
				}
				data := make([]byte, int(length))
				if _, err = reader.Read(data); err != nil {
					return result, err
				}
				kv = append(kv, data)
			}
			result = append(result, string(kv[0]), string(kv[1]))
		case opCodeEOF:
			// Get the 8-byte checksum after this
			checksum := make([]byte, 8)
			_, err := reader.Read(checksum)
			if err != nil {
				return result, err
			}
			return result, nil
		default:
			// Handle any other tags.
		}
	}
}

func decodeLength(r *bytes.Reader) (int, error) {
	num, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch {
	case num <= 63: // leading bits 00
		// Remaining 6 bits are the length.
		return int(num & 0b00111111), nil
	case num <= 127: // leading bits 01
		// Remaining 6 bits plus next byte are the length
		nextNum, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint16([]byte{num & 0b00111111, nextNum})
		return int(length), nil
	case num <= 191: // leading bits 10
		// Next 4 getBytes are the length
		getBytes := make([]byte, 4)
		_, err := r.Read(getBytes)
		if err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint32(getBytes)
		return int(length), nil
	case num <= 255: // leading bits 11
		// Next 6 bits indicate the format of the encoded object.
		// TODO: This will result in problems on the next read, possibly.
		valueType := num & 0b00111111
		return int(valueType), nil
	default:
		return 0, err
	}
}

func sliceIndex(data []byte, sep byte) int {
	for i, b := range data {
		if b == sep {
			return i
		}
	}
	return -1
}
func parseTable(bytes []byte) []byte {
	start := sliceIndex(bytes, opCodeResizeDB)
	end := sliceIndex(bytes, opCodeEOF)
	return bytes[start:end]
}

//func readFile(path string) string {
//	c, _ := os.ReadFile(path)
//	key := parseTable(c)
//	str := key[4 : 4+key[3]]
//	return string(str)
//}
