package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

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

func handleCommand(ctx RequestContext) {
	raw := ctx.Decode()
	command, args := strings.ToUpper(raw[0]), raw[1:]

	switch command {
		case "PING":
			ctx.conn.Write([]byte(ToSimpleString("PONG")))
		case "ECHO":
			ctx.conn.Write([]byte(ToBulkString(args[0])))
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
			
		go handleConnection(conn)
	}
}
