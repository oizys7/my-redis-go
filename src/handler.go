package main

import "sync"

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hSet,
	"HGET":    hGet,
	"HGETALL": hGetAll,
}

func ping(args []Value) Value {
	return Value{typ: STRING, str: "PONG"}
}

var SETs = map[string]string{}

// SETsMu 获取 SETs 的读写互斥锁
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	defer SETsMu.Unlock()

	return Value{typ: STRING, str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].bulk
	SETsMu.RLock()
	value, ok := SETs[key]
	defer SETsMu.RUnlock()

	if !ok {
		return Value{typ: NULL}
	}
	return Value{typ: STRING, str: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hSet(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'hset' command"}
	}
	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	_, ok := HSETs[hash]
	if !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
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
	value, ok := HSETs[hash][key]
	defer HSETsMu.RUnlock()

	if !ok {
		return Value{typ: NULL}
	}
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
	for k, v := range value {
		values = append(values, Value{typ: BULK, bulk: k})
		values = append(values, Value{typ: BULK, bulk: v})
	}
	return Value{typ: ARRAY, array: values}
}
