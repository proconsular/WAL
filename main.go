package main

import (
	"io/ioutil"
	"net/http"
)

var sm *SegmentManager

func main() {
	sm = makeManager(5)

	sm.init()
	go sm.run()

	http.HandleFunc("/", handleIndex)
	http.ListenAndServe(":4000", nil)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]
	if r.Method == http.MethodPost {
		value, _ := ioutil.ReadAll(r.Body)
		sm.write(key, string(value))
		w.Write([]byte("ok"))
	} else if r.Method == http.MethodGet {
		value := sm.read(key)
		if value == "" {
			w.Write([]byte("Not found."))
		} else {
			w.Write([]byte(value))
		}
	}
}
