package main

import (
	"testing"
	"time"
)

func TestKeyValueStore_SetGet(t *testing.T) {
	kv := NewKeyValueStore()

	kv.Set("key1", "value1", nil)
	val, exists := kv.Get("key1")
	if !exists || val != "value1" {
		t.Errorf("Expected value1, got %s (exists: %v)", val, exists)
	}

	val, exists = kv.Get("nonexistent")
	if exists {
		t.Errorf("Expected nonexistent to not exist")
	}
}

func TestKeyValueStore_Expiration(t *testing.T) {
	kv := NewKeyValueStore()

	expiry := time.Now().Add(500 * time.Millisecond)
	kv.Set("key1", "value1", &expiry)

	val, exists := kv.Get("key1")
	if !exists || val != "value1" {
		t.Errorf("Expected value1 to exist before expiration")
	}

	time.Sleep(600 * time.Millisecond)

	val, exists = kv.Get("key1")
	if exists {
		t.Errorf("Expected key1 to be expired")
	}
}

func TestKeyValueStore_EvictExpiredKeys(t *testing.T) {
	kv := NewKeyValueStore()

	expiry := time.Now().Add(100 * time.Millisecond)
	kv.Set("key1", "value1", &expiry)
	kv.Set("key2", "value2", nil)

	time.Sleep(200 * time.Millisecond)

	count := kv.EvictExpiredKeys()
	if count != 1 {
		t.Errorf("Expected 1 key to be evicted, got %d", count)
	}

	if kv.Exists("key1") {
		t.Errorf("Expected key1 to be evicted")
	}

	if !kv.Exists("key2") {
		t.Errorf("Expected key2 to still exist")
	}
}

func TestKeyValueStore_Concurrency(t *testing.T) {
	kv := NewKeyValueStore()
	done := make(chan bool)
	numWorkers := 10
	numOps := 1000

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for j := 0; j < numOps; j++ {
				key := "key"
				kv.Set(key, "value", nil)
				kv.Get(key)
				kv.Delete(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
}
