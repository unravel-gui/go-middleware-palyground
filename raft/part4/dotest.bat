@echo off
setlocal enabledelayedexpansion

set "logfile=E:\\temp\\part4\rlog"

go test -v -race -run $args 2>&1 >> %logfile%

type %logfile%

go run ..\tools\raft-testlog-viz\main.go < %logfile%
