// Package store 提供本地内存存储，用于模拟“实际资源状态”。
// 每个资源带单调递增的版本号，所有写操作基于 CAS（比较并交换），
// 以此支持执行前漂移检测与并发安全。
package store

import (
	"errors"
	"maps"
	"sync"
)

// 存储层错误，供上层归类为失败原因。
var (
	ErrNotFound        = errors.New("store: resource not found")
	ErrAlreadyExists   = errors.New("store: resource already exists")
	ErrVersionConflict = errors.New("store: version conflict")
)

// Resource 表示一个实际存在的资源实例。
type Resource struct {
	Name    string         `json:"name"`
	Kind    string         `json:"kind"`
	Spec    map[string]any `json:"spec"`
	Version uint64         `json:"version"`
	// ManagedBy 标记该资源由哪个控制器管理，用于识别“多余资源”。
	ManagedBy string `json:"managedBy,omitempty"`
}

// Snapshot 是某一时刻实际状态的一致性快照。
type Snapshot struct {
	Resources  map[string]Resource
	Generation uint64
}

// Op 描述一次写操作，供 Hook 观察。
type Op struct {
	Kind string // "apply" | "delete"
	Name string
}

// Hook 用于测试注入故障或模拟外部改动。
// 钩子在写操作获取锁之前调用，因此可以安全地调用 Store 的其他方法
// （例如模拟“计划生成后、执行前”由外部 actor 改动其他资源）。
// 注意：钩子内对 Store 的写会再次触发钩子，测试需自行防止递归。
type Hook func(op Op, r *Resource) error

// Store 是并发安全的内存资源存储。
type Store struct {
	mu         sync.Mutex
	resources  map[string]Resource
	generation uint64
	hook       Hook
}

// New 创建空存储。
func New() *Store {
	return &Store{resources: map[string]Resource{}}
}

// SetHook 设置在每次写操作生效前调用的钩子（仅测试用）。
// 钩子返回非 nil 错误时，本次写操作被拒绝且状态不变。
func (s *Store) SetHook(h Hook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hook = h
}

func (s *Store) getHook() Hook {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hook
}

// Get 读取单个资源。
func (s *Store) Get(name string) (Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.resources[name]
	if !ok {
		return Resource{}, ErrNotFound
	}
	return cloneResource(r), nil
}

// Snapshot 返回当前状态的一致性快照（深拷贝）。
func (s *Store) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Resource, len(s.resources))
	for k, v := range s.resources {
		out[k] = cloneResource(v)
	}
	return Snapshot{Resources: out, Generation: s.generation}
}

// Apply 以 CAS 方式创建或更新资源。
// expectedVersion 为 0 表示要求资源不存在（创建语义），
// 否则要求现存版本与之相等（更新语义）。
// 幂等：若期望内容与现存内容（含 ManagedBy）完全一致，
// 则不产生任何变更、不递增版本号，直接返回现存资源。
func (s *Store) Apply(name string, expectedVersion uint64, res Resource) (Resource, error) {
	res.Name = name
	if h := s.getHook(); h != nil {
		if err := h(Op{Kind: "apply", Name: name}, &res); err != nil {
			return Resource{}, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cur, exists := s.resources[name]
	if expectedVersion == 0 {
		if exists {
			return Resource{}, ErrAlreadyExists
		}
	} else {
		if !exists {
			return Resource{}, ErrNotFound
		}
		if cur.Version != expectedVersion {
			return Resource{}, ErrVersionConflict
		}
	}

	// 幂等：内容一致则不变更。
	if exists && cur.Kind == res.Kind && cur.ManagedBy == res.ManagedBy &&
		maps.EqualFunc(cur.Spec, res.Spec, func(a, b any) bool { return deepEqual(a, b) }) {
		return cloneResource(cur), nil
	}

	s.generation++
	res.Version = s.generation
	s.resources[name] = res
	return cloneResource(res), nil
}

// Delete 以 CAS 方式删除资源。
func (s *Store) Delete(name string, expectedVersion uint64) error {
	if h := s.getHook(); h != nil {
		if err := h(Op{Kind: "delete", Name: name}, nil); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cur, exists := s.resources[name]
	if !exists {
		return ErrNotFound
	}
	if cur.Version != expectedVersion {
		return ErrVersionConflict
	}
	s.generation++
	delete(s.resources, name)
	return nil
}

func cloneResource(r Resource) Resource {
	if r.Spec != nil {
		spec := make(map[string]any, len(r.Spec))
		for k, v := range r.Spec {
			spec[k] = v
		}
		r.Spec = spec
	}
	return r
}

// deepEqual 对 spec 值做可复现的比较（仅支持 JSON 风格的标量/切片/映射）。
func deepEqual(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, ok := bv[k]
			if !ok || !deepEqual(v, bvv) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
