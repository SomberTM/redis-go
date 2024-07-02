package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var kv map[string] string = make(map[string]string)

type RequestContext struct {
	conn net.Conn
	raw []byte
}

func (ctx *RequestContext) Decode() []string {
	raw_string := string(ctx.raw)
	parts := strings.Split(raw_string, "\r\n")

	args := make([]string, 0, 16)

	if len(parts) == 0 {
		return args
	}

	n, err := strconv.Atoi(parts[0][1:])
	if err != nil {
		fmt.Println("Error parsing RESP array length: ", err.Error())
	}

	if n == 0 {
		return args
	}

	for i := 2; i < len(parts); i += 2 {
		args = append(args, parts[i])
	}

	return args
}

func handleConnection(conn net.Conn) {
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				conn.Close()	
				break;
			}

			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		if n == 0 {
			break
		}

		ctx := RequestContext {
			conn: conn,
			raw: buf,
		}

		handleCommand(ctx)
	}
}

func deleteKeyAfter(key string, ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	delete(kv, key)
}

func handleCommand(ctx RequestContext) {
	raw := ctx.Decode()
	command, args := strings.ToUpper(raw[0]), raw[1:]

	switch command {
		case "PING":
			ctx.conn.Write([]byte(ToSimpleString("PONG")))
		case "ECHO":
			ctx.conn.Write([]byte(ToBulkString(args[0])))
		case "SET":
			kv[args[0]] = args[1]
			if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
				ms, err := strconv.Atoi(args[3])

				if err != nil {
					fmt.Println("Error parsing expiry: ", err.Error())
					ctx.conn.Write([]byte(ToSimpleError("Bad request")))
					return
				}

				go deleteKeyAfter(args[0], ms)
			}
			ctx.conn.Write([]byte(OkSimpleString))
		case "GET":
			v, ok := kv[args[0]]
			if ok {
				ctx.conn.Write([]byte(ToBulkString(v)))
			} else {
				ctx.conn.Write([]byte(NilBulkString))
			}
		case "INFO":
			if len(args) == 0 {
				ctx.conn.Write([]byte(ToSimpleError("Invalid INFO usage")))
				return
			}

			switch strings.ToUpper(args[0]) {
				case "REPLICATION":
					ctx.conn.Write([]byte(ToBulkString("# Replication\nrole:master\nconnected_slaves:0\nmaster_replid:hellomom\nmaster_repl_offset:0\n")))
				default:
					ctx.conn.Write([]byte(ToSimpleError("Unsupported INFO argument")))
			}
		default:
			ctx.conn.Write([]byte(ToSimpleError("Unsupported command")))
	}
}

func pargsToMap() map[string] string {
	args := os.Args[1:]
	argmap := make(map[string] string)

	if len(args) == 0 {
		return argmap
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			arg_value := args[i + 1]
			argmap[arg[2:]] = arg_value
			i++
		}
	}

	return argmap
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	args := pargsToMap()

	port, ok := args["port"]
	if !ok {
		port = "6379"
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	fmt.Println("Listening on port", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
			
		go handleConnection(conn)
	}
}
