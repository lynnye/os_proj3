#!/bin/bash
go build bin/http_backup.go
go build bin/http_server.go
chmod +x bin/start_server
chmod +x bin/stop_server

