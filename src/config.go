package main

import (
	"flag"
	"strings"
)

var port = flag.String("port", "6379", "port to listen on")
var dir = flag.String("dir", "", "Directory to store RDB file")
var dbFileName = flag.String("dbfilename", "dump.rdb", "RDB file name")

var Configs = map[string]string{}

func initConfigs() {
	// 解析命令行参数
	flag.Parse()

	Configs["port"] = *port
	Configs["dir"] = *dir
	Configs["dbfilename"] = *dbFileName
}

func configGet(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'config get' command"}
	}
	cmd := args[0].bulk
	if strings.ToUpper(cmd) != "GET" {
		return Value{typ: ERROR, str: "ERR unknown command '" + cmd + "'"}
	}
	key := args[1].bulk
	SETsMu.RLock()
	value, ok := Configs[key]
	defer SETsMu.RUnlock()
	if !ok {
		return Value{typ: NULL}
	}
	var values []Value
	values = append(values, Value{typ: BULK, bulk: key})
	values = append(values, Value{typ: BULK, bulk: value})
	return Value{typ: ARRAY, array: values}
}
