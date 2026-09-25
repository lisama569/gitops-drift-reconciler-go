package store

import (
	"errors"
	"sync"
	"testing"
)

func TestApplyCASAndIdempotency(t *testing.T) {
	s := New()

	// 创建：expectedVersion=0。
	r1, err := s.Apply("a", 0, Resource{Kind: "K", Spec: map[string]any{"v": 1.0}})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 1 {
		t.Fatalf("want version 1, got %d", r1.Version)
	}

	// 重复创建被拒绝。
	if _, err := s.Apply("a", 0, Resource{Kind: "K"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}

	// 版本过期被拒绝。
	if _, err := s.Apply("a", 99, Resource{Kind: "K"}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}

	// 幂等：内容一致时不产生版本变更。
	r2, err := s.Apply("a", 1, Resource{Kind: "K", Spec: map[string]any{"v": 1.0}})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Version != 1 {
		t.Fatalf("idempotent apply must not bump version, got %d", r2.Version)
	}
	if gen := s.Snapshot().Generation; gen != 1 {
		t.Fatalf("idempotent apply must not bump generation, got %d", gen)
	}

	// 内容变化才递增版本。
	r3, err := s.Apply("a", 1, Resource{Kind: "K", Spec: map[string]any{"v": 2.0}})
	if err != nil {
		t.Fatal(err)
	}
	if r3.Version != 2 {
		t.Fatalf("want version 2, got %d", r3.Version)
	}
	t.Logf("CAS + idempotency verified: versions 1 -> 1 (noop) -> 2 (real change)")
}

func TestDeleteCAS(t *testing.T) {
	s := New()
	if _, err := s.Apply("a", 0, Resource{Kind: "K"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("a", 99); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}
	if err := s.Delete("a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := s.Delete("a", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting missing resource: want ErrNotFound, got %v", err)
	}
}

func TestConcurrentApplyNoDuplicateCreate(t *testing.T) {
	s := New()
	const n = 16
	var wg sync.WaitGroup
	wins := 0
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Apply("x", 0, Resource{Kind: "K"}); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("exactly one create may win, got %d", wins)
	}
	if gen := s.Snapshot().Generation; gen != 1 {
		t.Fatalf("exactly one mutation expected, generation=%d", gen)
	}
	t.Logf("%d concurrent creates => 1 winner, generation=1", n)
}
