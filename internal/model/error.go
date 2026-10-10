package model

import (
	"encoding/json"
	"net/http"
)

// OpenAIErrorBody mirrors the OpenAI error object payload.
type OpenAIErrorBody struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param,omitempty"`
	Code    any     `json:"code,omitempty"`
}

// OpenAIErrorEnvelope wraps the error body in the standard OpenAI top-level envelope.
type OpenAIErrorEnvelope struct {
	Error OpenAIErrorBody `json:"error"`
}

// WriteJSONError formats and sends an OpenAI-compatible error response.
func WriteJSONError(w http.ResponseWriter, statusCode int, message, errType string, code any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	envelope := OpenAIErrorEnvelope{
		Error: OpenAIErrorBody{
			Message: message,
			Type:    errType,
			Code:    code,
		},
	}
	_ = json.NewEncoder(w).Encode(envelope)
}
