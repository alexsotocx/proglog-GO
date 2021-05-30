package main

import (
	"log"

	"github.com/alexsotocx/proglog/internal/server"
)

func main() {
	server := server.NewHTTPServer(":8080")
	log.Fatal(server.ListenAndServe())
}
