package main

import (
	"bytes"
	"fmt"
)

const OkSimpleString = "+OK\r\n" 
const NilBulkString = "$-1\r\n"

func ToSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func ToSimpleError(s string) string {
	return fmt.Sprintf("-%s\r\n", s)
}

func ToBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func ToRespArray(s []string) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("*%d\r\n", len(s)))
	for i := 0; i < len(s); i++ {
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(s[i]), s[i]))
	}
	return buf.String()
}