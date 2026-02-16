package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// KeyValueStore represents an in-memory key-value store with expiration support
type KeyValueStore struct {
	data map[string]ValueEntry
}

// ValueEntry represents a stored value with optional expiration
type ValueEntry struct {
	Value      string
	Expiration *time.Time
}

// NewKeyValueStore creates a new key-value store
func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]ValueEntry),
	}
}

// Set stores a key-value pair with optional expiration
func (kv *KeyValueStore) Set(key, value string, expiry *time.Time) {
	kv.data[key] = ValueEntry{
		Value:      value,
		Expiration: expiry,
	}
}

// Get retrieves a value by key, returning the value and whether it exists and hasn't expired
func (kv *KeyValueStore) Get(key string) (string, bool) {
	entry, exists := kv.data[key]
	if !exists {
		return "", false
	}

	// Check if the entry has expired
	if entry.Expiration != nil && time.Now().After(*entry.Expiration) {
		delete(kv.data, key)
		return "", false
	}

	return entry.Value, true
}

// Exists checks if a key exists and hasn't expired
func (kv *KeyValueStore) Exists(key string) bool {
	_, exists := kv.Get(key)
	return exists
}

// Delete removes a key from the store
func (kv *KeyValueStore) Delete(key string) int {
	_, exists := kv.data[key]
	if exists {
		delete(kv.data, key)
		return 1
	}
	return 0
}

// Expire sets an expiration time for a key
func (kv *KeyValueStore) Expire(key string, seconds int) bool {
	entry, exists := kv.data[key]
	if !exists {
		return false
	}

	expiration := time.Now().Add(time.Duration(seconds) * time.Second)
	entry.Expiration = &expiration
	kv.data[key] = entry
	return true
}

func main() {
	fmt.Println("Starting Redis-compatible server on port 6379...")
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	
	fmt.Println("Server is ready to accept connections")

	// In-memory storage for key-value pairs
	store := NewKeyValueStore()

	// Accept connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		
		// Handle each connection
		go handleConnection(conn, store)
	}
}

func handleConnection(conn net.Conn, store *KeyValueStore) {
	defer conn.Close()
	
	// Log connection
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("New connection from %s\n", clientAddr)
	
	reader := bufio.NewReader(conn)
	
	for {
		// Read the RESP array length (first line)
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Client %s disconnected\n", clientAddr)
				return
			}
			fmt.Printf("Error reading from connection %s: %v\n", clientAddr, err)
			return
		}
		
		line = strings.TrimSuffix(line, "\r\n")
		
		if len(line) == 0 {
			continue
		}
		
		// Check if it's an array
		if line[0] == '*' {
			arrayLen, err := strconv.Atoi(line[1:])
			if err != nil {
				conn.Write([]byte("-ERR Protocol error\r\n"))
				continue
			}
			
			// Read the array elements
			args := make([]string, 0, arrayLen)
			for i := 0; i < arrayLen; i++ {
				// Read bulk string length
				bulkLine, err := reader.ReadString('\n')
				if err != nil {
					conn.Write([]byte("-ERR Protocol error\r\n"))
					break
				}
				bulkLine = strings.TrimSuffix(bulkLine, "\r\n")
				
				if bulkLine[0] != '$' {
					conn.Write([]byte("-ERR Protocol error\r\n"))
					break
				}
				
				// Check for null bulk string
				if bulkLine == "$-1" {
					args = append(args, "")
					continue
				}
				
				// Parse bulk string length
				bulkLen, err := strconv.Atoi(bulkLine[1:])
				if err != nil {
					conn.Write([]byte("-ERR Protocol error\r\n"))
					break
				}
				
				// Read the actual string
				if bulkLen == 0 {
					// Read empty line
					emptyLine, err := reader.ReadString('\n')
					if err != nil {
						conn.Write([]byte("-ERR Protocol error\r\n"))
						break
					}
					emptyLine = strings.TrimSuffix(emptyLine, "\r\n")
					args = append(args, "")
				} else {
					strBytes := make([]byte, bulkLen+2) // +2 for \r\n
					_, err = io.ReadFull(reader, strBytes)
					if err != nil {
						conn.Write([]byte("-ERR Protocol error\r\n"))
						break
					}
					strLine := string(strBytes[:bulkLen]) // Exclude \r\n
					args = append(args, strLine)
				}
			}
			
			if len(args) > 0 {
				executeCommand(conn, args, store)
			}
		} else {
			// Handle inline commands for simpler cases
			parts := strings.Fields(line)
			if len(parts) > 0 {
				executeCommand(conn, parts, store)
			}
		}
	}
}

func executeCommand(conn net.Conn, args []string, store *KeyValueStore) {
	command := strings.ToUpper(args[0])
	
	// Log command execution
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("Client %s executing command: %s\n", clientAddr, command)
	
	switch command {
	case "PING":
		if len(args) > 1 {
			// PING with message
			message := args[1]
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)))
		} else {
			// Simple PING
			conn.Write([]byte("+PONG\r\n"))
		}
	case "ECHO":
		if len(args) > 1 {
			message := args[1]
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
		}
	case "SET":
		if len(args) >= 3 {
			key := args[1]
			value := args[2]
			
			// Check for expiration options
			var expiry *time.Time
			for i := 3; i < len(args); i += 2 {
				option := strings.ToUpper(args[i])
				if option == "PX" && i+1 < len(args) {
					// PX - expire in milliseconds
					ms, err := strconv.Atoi(args[i+1])
					if err != nil {
						conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
						return
					}
					expTime := time.Now().Add(time.Duration(ms) * time.Millisecond)
					expiry = &expTime
				} else if option == "EX" && i+1 < len(args) {
					// EX - expire in seconds
					seconds, err := strconv.Atoi(args[i+1])
					if err != nil {
						conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
						return
					}
					expTime := time.Now().Add(time.Duration(seconds) * time.Second)
					expiry = &expTime
				}
			}
			
			store.Set(key, value, expiry)
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		}
	case "GET":
		if len(args) >= 2 {
			key := args[1]
			if value, exists := store.Get(key); exists {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		}
	case "EXISTS":
		if len(args) >= 2 {
			count := 0
			for i := 1; i < len(args); i++ {
				if store.Exists(args[i]) {
					count++
				}
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'exists' command\r\n"))
		}
	case "DEL":
		if len(args) >= 2 {
			count := 0
			for i := 1; i < len(args); i++ {
				count += store.Delete(args[i])
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
		}
	case "EXPIRE":
		if len(args) >= 3 {
			key := args[1]
			seconds, err := strconv.Atoi(args[2])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				return
			}
			
			if store.Expire(key, seconds) {
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'expire' command\r\n"))
		}
	case "INCR":
		if len(args) >= 2 {
			key := args[1]
			if value, exists := store.Get(key); exists {
				intVal, err := strconv.Atoi(value)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer\r\n"))
					return
				}
				intVal++
				strVal := strconv.Itoa(intVal)
				store.Set(key, strVal, nil)
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", intVal)))
			} else {
				store.Set(key, "1", nil)
				conn.Write([]byte(":1\r\n"))
			}
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'incr' command\r\n"))
		}
	default:
		conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
	}
}