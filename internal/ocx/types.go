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

// PermissionRequest is an unresolved OpenCode permission request. The
// global permission endpoint is durable, so adapters can reconcile requests
// that were emitted while the SSE stream was disconnected.
type PermissionRequest struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionID"`
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

// MessageSummary is role-dependent on the OpenCode wire: user messages use
// an object containing diffs, while compacted assistant messages use a bool.
// Keeping both shapes decodable prevents one compacted assistant message from
// making the entire transcript unreadable.
type MessageSummary struct {
	Title     string     `json:"title"`
	Body      string     `json:"body"`
	Diffs     []FileDiff `json:"diffs"`
	Compacted bool       `json:"-"`
}

func (s *MessageSummary) UnmarshalJSON(data []byte) error {
	var compacted bool
	if err := json.Unmarshal(data, &compacted); err == nil {
		s.Compacted = compacted
		s.Title = ""
		s.Body = ""
		s.Diffs = nil
		return nil
	}
	type summary MessageSummary
	var decoded summary
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*s = MessageSummary(decoded)
	return nil
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
	Summary   *MessageSummary  `json:"summary"`
	Time      struct {
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
