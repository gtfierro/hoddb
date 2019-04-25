package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

var dbfile = flag.String("dbfile", "/etc/answers.db", "Location of database")

func main() {
	flag.Parse()
	db, err := sql.Open("sqlite3", *dbfile)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS answers (
        id  INTEGER PRIMARY KEY,
        inserted DATETIME DEFAULT CURRENT_TIMESTAMP,
        answer JSON
    );`)
	if err != nil {
		log.Fatal(err)
	}

	//fs4 := http.StripPrefix("/viz/", http.FileServer(http.Dir("viz")))
	http.Handle("/", http.FileServer(http.Dir("viz")))
	http.HandleFunc("/postform", func(rw http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		var v interface{}
		dec := json.NewDecoder(req.Body)
		if err := dec.Decode(&v); err != nil {
			http.Error(rw, err.Error(), 400)
			return
		}
		log.Printf("Got %+v", v)
		bytes, err := json.Marshal(v)
		if err != nil {
			http.Error(rw, err.Error(), 400)
			return
		}
		stmt := "INSERT INTO answers(answer) VALUES (?);"
		if _, err := db.Exec(stmt, string(bytes)); err != nil {
			http.Error(rw, err.Error(), 400)
			return
		}
	})

	log.Println("Listening on :3000...")
	log.Fatal(http.ListenAndServe(":3000", nil))
}
