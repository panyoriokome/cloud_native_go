package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

// var store = make(map[string]string)
var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

var ErrorNoSuchKey = errors.New("no such key")

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Request Bodyからキーの値を取得する
	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	// キーの値の取得に失敗したらInternal Server Errorを返す
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	// Putに失敗したらInternal Server Errorを返す
	err = Put(key, string(value))
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	// キーが見つからなければNot Foundを返す
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.Write([]byte("Delete Compeleted"))

}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func Put(key string, value string) error {
	// store[key] = value
	store.Lock()
	store.m[key] = value
	store.Unlock()

	return nil
}

func Get(key string) (string, error) {
	// value, ok := store[key]
	store.Lock()
	value, ok := store.m[key]
	store.Unlock()

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	store.Lock()
	// delete(store, key)
	delete(store.m, key)
	store.Unlock()

	return nil
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
}

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type FileTransactionLogger struct {
	// something
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func (I *FileTransactionLogger) WritePut(key, value string) {
	// something
	I.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (I *FileTransactionLogger) WriteDelete(key, value string) {
	// something
	I.events <- Event{EventType: EventDelete, Key: key}
}

func (I *FileTransactionLogger) Err() <-chan error {
	return I.errors
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (I *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	I.events = events

	errors := make(chan error, 1)
	I.errors = errors

	go func() {
		for e := range events {
			I.lastSequence++

			_, err := fmt.Fprintf(
				I.file,
				"%d\t%d\t%s\t\n",
				I.lastSequence, e.EventType, e.Key, e.Value,
			)

			if err != nil {
				errors <- err
				return
			}
		}
	}()
}
