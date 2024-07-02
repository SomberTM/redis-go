package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type RespType int

const (
	SimpleString RespType = iota
	SimpleError
	Integer
	BulkString
	Array
	Null
	Boolean
	Double
	BigNumber
	BulkError
	VerbatimString
	Map
	Set
	Push
)

func getRespType(s []byte) RespType {
	switch s[0] {
		case '+': return SimpleString
		case '-': return SimpleError
		case ':': return Integer
		case '$': return BulkString 
		case '*': return Array
		case '_': return Null
		case '#': return Boolean
		case ',': return Double
		case '(': return BigNumber
		case '!': return BulkError
		case '=': return VerbatimString
		case '%': return Map
		case '~': return Set
		case '>': return Push
		default:
			return -1
	}
}

type RespData struct {
	data_type RespType
	value interface{}
}

type RespParser struct {
	raw []byte
	segments [][]byte
	idx int
}

func NewRespParser(b []byte) RespParser {
	return RespParser{
		raw: b,
		segments: bytes.Split(b, []byte{'\r', '\n'}),
		idx: 0,
	}
}

func (parser *RespParser) getCurrentType() RespType {
	return getRespType(parser.current())
}

func (parser *RespParser) current() []byte {
	return parser.segments[parser.idx]
}

func (parser *RespParser) Done() bool {
	return parser.idx >= len(parser.segments) - 1
}

func (parser *RespParser) Reset() {
	parser.idx = 0
}

func (parser *RespParser) Next() (RespData, error) {
	var data RespData;
	switch parser.getCurrentType() {
		case Array:
			arr, err := parser.NextArray()
			if err != nil {
				return data, err
			}
			data.data_type = Array
			data.value = arr
		case SimpleString:
			str, err := parser.NextSimpleString()
			if err != nil {
				return data, err
			}
			data.data_type = Array
			data.value = str
		case BulkString:
			str, err := parser.NextBulkString()
			if err != nil {
				return data, err
			}
			data.data_type = Array
			data.value = str
		default:
			return data, io.EOF
	}

	return data, nil 
}

func (parser *RespParser) NextSimpleString() ([]byte, error) {
	if parser.Done() {
		return nil, io.EOF
	}

	resp_type := parser.getCurrentType()
	if resp_type != SimpleString {
		return nil, errors.New("current item is not a resp simple string")
	}

	return parser.current()[1:], nil
}

func (parser *RespParser) NextBulkString() ([]byte, error) {
	if parser.Done() {
		return nil, io.EOF
	}

	resp_type := parser.getCurrentType()
	if resp_type != BulkString {
		return nil, errors.New("current item is not a resp bulk string")
	}

	parser.idx++
	str := parser.current()
	parser.idx++
	return str, nil
}

func (parser *RespParser) NextArray() ([][]byte, error) {
	if parser.Done() {
		return nil, io.EOF
	}

	resp_type := parser.getCurrentType() 
	if resp_type != Array {
		return nil, errors.New("current item is not a resp array")
	}

	cur := parser.current()
	arrlen, err := strconv.Atoi(string(cur[1:]))
	if err != nil {
		return nil, err
	}

	arr := make([][]byte, 0, arrlen)

	if arrlen <= 0 {
		return arr, nil
	}

	parser.idx++

	for i := 0; i < arrlen; i++ {
		str, err := parser.NextBulkString()
		if err != nil {
			fmt.Println("Not implemented")
		}
		arr = append(arr, str)
	}

	return arr, nil
}
