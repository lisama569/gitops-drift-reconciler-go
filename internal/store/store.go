// Package store 用本地内存模拟实际资源状态。
// 所有写操作基于全局版本号做乐观并发控制：
// 调用方必须携带计划生成时观察到的 baseVersion，
// 若期间实际状态被外部改动，写入会被拒绝（冲突），
// 从而保证过期计划不会被套用到已变化的资源上。
package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
)

// ErrConflict 表示写入时版本冲突（实际状态已被并发修改）。
var ErrConflict = errors.New("store: 版本冲突，实际状态已被修改")

// ErrAlreadyExists 表示并发创建同一资源时本执行者未获胜：
// 资源已被其他执行者创建，本次创建被明确拒绝而非静默覆盖。
var ErrAlreadyExists = errors.New("store: 资源已存在，创建冲突")

// Record 是实际状态中的一条资源记录。
type Record struct {
	Resource decl.Resource
	Version  uint64 // 该记录最后被写入时的全局版本
}

// Store 是线程安全的本地状态存储。
type Store struct {
	mu      sync.Mutex
	records map[string]Record
	version uint64 // 全局单调递增版本，每次成功写入 +1
}

// New 创建空存储。
func New() *Store {
	return &Store{records: make(map[string]Record)}
}

// Snapshot 返回当前全部记录的拷贝及全局版本快照。
func (s *Store) Snapshot() (map[string]Record, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Record, len(s.records))
	for k, v := range s.records {
		out[k] = v
	}
	return out, s.version
}

// Apply 以乐观并发方式写入（创建或更新）资源。
// baseVersion 必须与当前全局版本一致，否则返回 ErrConflict。
// 返回写入后的全局版本。
func (s *Store) Apply(res decl.Resource, baseVersion uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if baseVersion != s.version {
		return s.version, fmt.Errorf("%w: 期望版本 %d，当前版本 %d", ErrConflict, baseVersion, s.version)
	}
	s.version++
	s.records[res.Key()] = Record{Resource: res, Version: s.version}
	return s.version, nil
}

// CreateIfAbsent 原子地创建资源：仅当 key 不存在时写入。
// 多个执行者同时创建同一资源时，只有一个成功，其余返回
// ErrAlreadyExists（明确冲突），不会静默覆盖或各自都以为成功。
// 返回写入后的全局版本。
func (s *Store) CreateIfAbsent(res decl.Resource) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := res.Key()
	if _, exists := s.records[key]; exists {
		return s.version, fmt.Errorf("%w: %s", ErrAlreadyExists, key)
	}
	s.version++
	s.records[key] = Record{Resource: res, Version: s.version}
	return s.version, nil
}

// Delete 删除资源，baseVersion 语义同 Apply。
func (s *Store) Delete(key string, baseVersion uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if baseVersion != s.version {
		return s.version, fmt.Errorf("%w: 期望版本 %d，当前版本 %d", ErrConflict, baseVersion, s.version)
	}
	s.version++
	delete(s.records, key)
	return s.version, nil
}

// Keys 返回当前所有资源 key（排序后，便于可复现输出）。
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
