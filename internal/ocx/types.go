package ocx

import "encoding/json"

type Session struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectID"`
	Directory string `json:"directory"`
	Title     string `json:"title"`
	Version   string `json:"version"`
	Time      struct {
		Created int64 `json:"created"`
		Updated int64 `json:"updated"`
	} `json:"time"`
}

type SessionStatus struct {
	Type    string `json:"type"`
	Attempt int    `json:"attempt"`
	Message string `json:"message"`
	Next    int    `json:"next"`
}

type FileDiff struct {
	File      string `json:"file"`
	Patch     string `json:"patch"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
	Status    string `json:"status"`
}

type TokenCount struct {
	Total     int `json:"total"`
	Input     int `json:"input"`
	Output    int `json:"output"`
	Reasoning int `json:"reasoning"`
	Cache     struct {
		Read  int `json:"read"`
		Write int `json:"write"`
	} `json:"cache"`
}

type MessageInfo struct {
	ID        string  `json:"id"`
	Role      string  `json:"role"`
	SessionID string  `json:"sessionID"`
	ParentID  string  `json:"parentID"`
	Finish    *string `json:"finish"`
	ModelID   string  `json:"modelID"`
	Provider  string  `json:"providerID"`
	Mode      string  `json:"mode"`
	Cost      float64 `json:"cost"`
	Tokens    TokenCount
	Error     *json.RawMessage `json:"error"`
	Summary   *struct {
		Title string     `json:"title"`
		Body  string     `json:"body"`
		Diffs []FileDiff `json:"diffs"`
	} `json:"summary"`
	Time struct {
		Created   int64  `json:"created"`
		Completed *int64 `json:"completed"`
	} `json:"time"`
}

type Message struct {
	Info  MessageInfo       `json:"info"`
	Parts []json.RawMessage `json:"parts"`
}

type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Properties json.RawMessage `json:"properties"`
	Directory  string          `json:"directory"`
	Project    string          `json:"project"`
}
