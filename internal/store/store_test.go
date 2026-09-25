package store

import (
	"errors"
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
