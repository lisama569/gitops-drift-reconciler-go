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
