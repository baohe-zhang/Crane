package utils

import (
	"encoding/json"
)

const (
	JOIN_REQUEST = "join_reqeust"
)

type PayloadHeader struct {
	Type string
}

type PayloadMessage struct {
	Header  PayloadHeader
	Content []byte
}

type JoinRequest struct {
	Name string
}

func Marshal(contentType string, content interface{}) ([]byte, error) {
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}

	msg := PayloadMessage{
		PayloadHeader{Type: contentType},
		contentBytes,
	}

	return json.Marshal(msg)
}

func CheckType(raw []byte) *PayloadMessage {
	payload := &PayloadMessage{}
	json.Unmarshal(raw, payload)
	return payload
}

func Unmarshal(raw []byte, content interface{}) {
	json.Unmarshal(raw, content)
}
