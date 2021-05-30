package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(addr string) *http.Server {
	httpServer := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", httpServer.handleProduce).Methods("POST")
	router.HandleFunc("/", httpServer.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(responseWriter http.ResponseWriter, request *http.Request) {
	var reqBody ProduceRequest
	err := json.NewDecoder(request.Body).Decode(&reqBody)
	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		return
	}
	offset, err := s.Log.Append(reqBody.Record)
	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{
		Offset: offset,
	}
	err = json.NewEncoder(responseWriter).Encode(res)
	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(responseWriter http.ResponseWriter, request *http.Request) {
	var reqBody ConsumeRequest
	err := json.NewDecoder(request.Body).Decode(&reqBody)
	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(reqBody.Offset)
	if err == ErrOffsetNotFound {
		http.Error(responseWriter, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(responseWriter).Encode(res)
	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		return
	}
}
