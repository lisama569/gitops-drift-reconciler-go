package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
)

func TestOptimisticConcurrency(t *testing.T) {
	s := New()
	res := decl.Resource{Kind: "App", Name: "a", Spec: map[string]string{"v": "1"}}

	_, base := s.Snapshot()
	v1, err := s.Apply(res, base)
	if err != nil {
		t.Fatalf("首次写入应成功: %v", err)
	}
	// 过期版本写入必须被拒绝。
	_, staleErr := s.Apply(res, base)
	if !errors.Is(staleErr, ErrConflict) {
		t.Fatalf("过期版本应返回冲突: %v", staleErr)
	}
	t.Logf("过期版本被拒绝: %v", staleErr)
	// 携带最新版本写入成功。
	res.Spec["v"] = "2"
	v2, err := s.Apply(res, v1)
	if err != nil {
		t.Fatalf("最新版本写入应成功: %v", err)
	}
	if v2 <= v1 {
		t.Fatalf("版本应单调递增: %d -> %d", v1, v2)
	}
	// 删除同样受版本保护。
	if _, err := s.Delete("App/a", v1); !errors.Is(err, ErrConflict) {
		t.Fatalf("过期版本删除应返回冲突: %v", err)
	}
	if _, err := s.Delete("App/a", v2); err != nil {
		t.Fatalf("最新版本删除应成功: %v", err)
	}
	if len(s.Keys()) != 0 {
		t.Fatalf("删除后应为空: %v", s.Keys())
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := New()
	res := decl.Resource{Kind: "App", Name: "a"}
	if _, err := s.Apply(res, 0); err != nil {
		t.Fatal(err)
	}
	snap, _ := s.Snapshot()
	// 修改快照不得影响存储内部状态。
	delete(snap, "App/a")
	if len(s.Keys()) != 1 {
		t.Fatal("快照应与存储隔离")
	}
}

// 并发创建同一资源：只有一个执行者成功，其余全部收到明确冲突，
// 不允许静默覆盖或各自都以为自己成功。
func TestConcurrentCreateIfAbsent(t *testing.T) {
	s := New()
	res := decl.Resource{Kind: "App", Name: "a", Spec: map[string]string{"v": "1"}}

	const workers = 16
	var wg sync.WaitGroup
	var successes, conflicts int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.CreateIfAbsent(res)
			switch {
			case err == nil:
				atomic.AddInt32(&successes, 1)
			case errors.Is(err, ErrAlreadyExists):
				atomic.AddInt32(&conflicts, 1)
			default:
				t.Errorf("意料之外的错误: %v", err)
			}
		}()
	}
	wg.Wait()

	t.Logf("并发创建结果: 成功 %d，明确冲突 %d", successes, conflicts)
	if successes != 1 {
		t.Fatalf("并发创建同一资源只能有一个成功，实际 %d", successes)
	}
	if conflicts != workers-1 {
		t.Fatalf("其余执行者必须全部收到明确冲突，实际 %d", conflicts)
	}
	// 最终状态可观察且唯一。
	if got := s.Keys(); len(got) != 1 || got[0] != "App/a" {
		t.Fatalf("最终应只有 App/a: %v", got)
	}
	// 已存在的资源再次创建仍被拒绝（不静默覆盖）。
	if _, err := s.CreateIfAbsent(decl.Resource{Kind: "App", Name: "a", Spec: map[string]string{"v": "999"}}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("重复创建应返回 ErrAlreadyExists: %v", err)
	}
	snap, _ := s.Snapshot()
	if snap["App/a"].Resource.Spec["v"] != "1" {
		t.Fatalf("已存在资源不得被静默覆盖: %v", snap["App/a"].Resource.Spec)
	}
}
