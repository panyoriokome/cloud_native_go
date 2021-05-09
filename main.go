package main

import (
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func helloGoHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello net/http!\n"))
}

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello gorilla/mux!\n"))
}

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

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")

	log.Fatal(http.ListenAndServe(":8080", r))
}

var store = make(map[string]string)

func Put(key string, value string) error {
	store[key] = value

	return nil
}

var ErrorNoSuchKey = errors.New("no such key")

func Get(key string) (string, error) {
	value, ok := store[key]

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	delete(store, key)

	return nil
}
