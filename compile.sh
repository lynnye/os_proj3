#!/bin/bash
go build -o bin/http_backup bin/http_backup.go
go build -o bin/http_server bin/http_server.go
chmod +x bin/start_server
chmod +x bin/stop_server

