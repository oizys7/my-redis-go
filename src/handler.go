package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"CONFIG":  configGet,
	"PING":    ping,
	"ECHO":    echo,
	"SET":     set,
	"GET":     get,
	"HSET":    hSet,
	"HGET":    hGet,
	"HGETALL": hGetAll,
	"KEYS":    keys,
}

type Entry struct {
	Type        string
	Value       any
	TimeCreated time.Time
	ExpiryInMS  time.Time
}

func ping(args []Value) Value {
	_ = args
	return Value{typ: STRING, str: "PONG"}
}

func echo(args []Value) Value {
	value := args[0].bulk
	return Value{typ: STRING, str: value}
}

// todo-w 如何将 SETs 和 HSETs 放在一起？数据结构如何设计？
// 将 SETs 设计为 key -> value，将 HSETs 设计为 hash:key -> value

// STORAGE 存储所有类型的数据
//type STORAGE struct {
//	Data map[string]*Entry
//	Mu   sync.RWMutex
//}
//
//var storage = STORAGE{}

var SETs = map[string]*Entry{}

// SETsMu 获取 SETs 的读写互斥锁
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 && len(args) != 4 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].bulk
	value := args[1].bulk
	now, expires := time.Now(), time.Time{}

	if len(args) == 4 {
		cmd := args[2].bulk
		var duration int64
		duration, _ = strconv.ParseInt(args[3].bulk, 10, 64)
		switch strings.ToLower(cmd) {
		case "ex":
			expires = now.Add(time.Duration(duration) * time.Second)
		case "px":
			expires = now.Add(time.Duration(duration) * time.Millisecond)
		default:
			return Value{typ: ERROR, str: "ERR unknown unit " + cmd + ", should be EX or PX"}
		}
	}

	//storage.Mu.Lock()
	//storage.Data[key] = &Entry{
	//	Type:        "SET",
	//	Value:       value,
	//	TimeCreated: now,
	//	ExpiryInMS:  expires,
	//}
	//defer storage.Mu.Unlock()
	SETsMu.Lock()
	SETs[key] = &Entry{
		Value:       value,
		TimeCreated: now,
		ExpiryInMS:  expires,
	}
	defer SETsMu.Unlock()

	return Value{typ: STRING, str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'get' command"}
	}
	key := args[0].bulk
	//storage.Mu.RLock()
	//entry, ok := storage.Data[key]
	//defer storage.Mu.RUnlock()

	SETsMu.RLock()
	entry, ok := SETs[key]
	defer SETsMu.RUnlock()

	if !ok || (entry.ExpiryInMS.Before(time.Now()) && entry.ExpiryInMS != time.Time{}) {
		return Value{typ: NULL}
	}
	var value, _ = anyToString(entry.Value)
	return Value{typ: STRING, str: value}
}

func anyToString(value any) (string, error) {
	// 使用类型断言检查是否为 string
	if str, ok := value.(string); ok {
		return str, nil
	}
	return "", fmt.Errorf("value is not a string: %v", value)
}

var HSETs = map[string]map[string]*Entry{}
var HSETsMu = sync.RWMutex{}

func hSet(args []Value) Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'hset' command"}
	}
	hash := args[0].bulk
	pair := (len(args) - 1) / 2

	HSETsMu.Lock()
	_, ok := HSETs[hash]
	if !ok {
		HSETs[hash] = map[string]*Entry{}
	}
	for i := 0; i < pair; i++ {
		key := args[1+i*2].bulk
		value := args[1+i*2+1].bulk
		HSETs[hash][key] = &Entry{
			Value:       value,
			TimeCreated: time.Now(),
			ExpiryInMS:  time.Time{},
		}
	}

	defer HSETsMu.Unlock()

	return Value{typ: STRING, str: "OK"}
}

func hGet(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'hget' command"}
	}
	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	entry, ok := HSETs[hash][key]
	defer HSETsMu.RUnlock()

	if !ok {
		return Value{typ: NULL}
	}
	var value, _ = anyToString(entry.Value)
	return Value{typ: BULK, bulk: value}
}

func hGetAll(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'hgetall' command"}
	}
	hash := args[0].bulk
	HSETsMu.RLock()
	value, ok := HSETs[hash]
	defer HSETsMu.RUnlock()

	if !ok {
		return Value{typ: NULL}
	}

	var values []Value
	for k, e := range value {
		var v, _ = anyToString(e.Value)
		values = append(values, Value{typ: BULK, bulk: k})
		values = append(values, Value{typ: BULK, bulk: v})
	}
	return Value{typ: ARRAY, array: values}
}

// Returns all keys matching pattern.
func keys(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'keys' command"}
	}
	key := args[0].bulk

	var value []Value
	if key == "*" {
		for setKey := range SETs {
			value = append(value, Value{typ: BULK, bulk: setKey})
		}
	} else {
		SETsMu.RLock()
		entry, ok := SETs[key]
		defer SETsMu.RUnlock()
		if !ok {
			return Value{typ: NULL}
		}
		valueString, _ := anyToString(entry.Value)
		value = append(value, Value{typ: BULK, bulk: valueString})
	}

	// 先支持SETs
	return Value{typ: ARRAY, array: value}
}
