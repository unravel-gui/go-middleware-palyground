@echo off
setlocal enabledelayedexpansion

set "logfile=E:\\temp\\part2\rlog"

go test -v -race -run $args 2>&1 >> %logfile%

type %logfile%

go run ..\tools\raft-testlog-viz\main.go < %logfile%
