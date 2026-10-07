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

// 多执行者并发创建同一资源：基于同一快照版本，只能有一个成功，
// 其余必须得到明确的 ErrConflict，不允许静默覆盖或各自都以为成功。
func TestConcurrentCreateSameKey(t *testing.T) {
	const workers = 16
	s := New()
	_, base := s.Snapshot()

	var success, conflict int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res := decl.Resource{Kind: "App", Name: "a", Spec: map[string]string{"writer": string(rune('A' + i))}}
			_, err := s.Apply(res, base)
			switch {
			case err == nil:
				atomic.AddInt32(&success, 1)
			case errors.Is(err, ErrConflict):
				atomic.AddInt32(&conflict, 1)
			default:
				t.Errorf("意外错误: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	t.Logf("并发创建同一资源: 成功=%d 冲突=%d", success, conflict)
	if success != 1 || conflict != workers-1 {
		t.Fatalf("应恰好 1 个成功、%d 个冲突，实际 成功=%d 冲突=%d", workers-1, success, conflict)
	}
	// 最终只有一条记录，内容完整（不存在写一半的交错状态）。
	records, ver := s.Snapshot()
	if len(records) != 1 {
		t.Fatalf("应只有 1 条记录: %v", s.Keys())
	}
	rec := records["App/a"]
	if len(rec.Resource.Spec) != 1 || rec.Resource.Spec["writer"] == "" {
		t.Fatalf("记录应完整: %+v", rec)
	}
	t.Logf("最终状态: version=%d 记录=%+v", ver, rec.Resource)
}
