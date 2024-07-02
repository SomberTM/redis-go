package main

import (
	"bytes"
	"fmt"
)

const OkSimpleString = "+OK\r\n" 
const NilBulkString = "$-1\r\n"

func ToSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func ToSimpleError(s string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", s))
}

func ToBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func ToRespArray(s []string) []byte {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("*%d\r\n", len(s)))
	for i := 0; i < len(s); i++ {
		buf.Write(ToBulkString(s[i]))
	}
	return buf.Bytes()
}