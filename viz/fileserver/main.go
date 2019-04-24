package main

import (
	"log"
	"net/http"
)

func main() {

	//fs4 := http.StripPrefix("/viz/", http.FileServer(http.Dir("viz")))
	http.Handle("/", http.FileServer(http.Dir("viz")))

	log.Println("Listening on :3000...")
	log.Fatal(http.ListenAndServe(":3000", nil))
}
