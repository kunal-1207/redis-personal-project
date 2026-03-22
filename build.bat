@echo off
REM Build script for Redis personal project on Windows

echo Building Redis server...
go build -o redis-server.exe cmd/main.go

if %ERRORLEVEL% EQU 0 (
    echo Build successful! Run redis-server.exe to start the server.
) else (
    echo Build failed!
)