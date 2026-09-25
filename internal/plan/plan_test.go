package plan

import (
	"testing"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/spec"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

func mustParse(t *testing.T, doc string) spec.DesiredState {
	t.Helper()
	d, err := spec.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return d
}

func TestDiffClassifiesChanges(t *testing.T) {
	d := mustParse(t, `{"resources": [
		{"name": "keep", "kind": "K", "spec": {"v": 1}},
		{"name": "drifted", "kind": "K", "spec": {"v": 2}},
		{"name": "missing", "kind": "K"}
	]}`)

	st := store.New()
	// keep：与期望一致
	if _, err := st.Apply("keep", 0, store.Resource{Kind: "K", Spec: map[string]any{"v": 1.0}, ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	// drifted：字段漂移
	if _, err := st.Apply("drifted", 0, store.Resource{Kind: "K", Spec: map[string]any{"v": 99.0}, ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	// extra：多余资源（本控制器管理但不在期望中）
	if _, err := st.Apply("extra", 0, store.Resource{Kind: "K", ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	// foreign：他人管理的资源，不应出现在计划中
	if _, err := st.Apply("foreign", 0, store.Resource{Kind: "K", ManagedBy: "other"}); err != nil {
		t.Fatal(err)
	}

	p, err := Diff(d, st.Snapshot(), "test")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]ChangeType{}
	for _, c := range p.Changes {
		got[c.Resource.Name] = c.Type
	}
	want := map[string]ChangeType{
		"keep": NoOp, "drifted": Update, "missing": Create, "extra": Delete,
	}
	for name, wt := range want {
		if got[name] != wt {
			t.Errorf("resource %s: want %s, got %s", name, wt, got[name])
		}
	}
	if _, ok := got["foreign"]; ok {
		t.Errorf("foreign-managed resource must not appear in plan")
	}
	t.Logf("classification: %v", got)
}

func TestDiffDeterministicOrder(t *testing.T) {
	d := mustParse(t, `{"resources": [
		{"name": "z-app", "kind": "K", "dependsOn": ["b-mid"]},
		{"name": "a-base", "kind": "K"},
		{"name": "b-mid", "kind": "K", "dependsOn": ["a-base"]},
		{"name": "c-independent", "kind": "K"}
	]}`)
	st := store.New()

	var orders [][]string
	for i := 0; i < 5; i++ {
		p, err := Diff(d, st.Snapshot(), "test")
		if err != nil {
			t.Fatal(err)
		}
		var order []string
		for _, c := range p.Changes {
			order = append(order, c.Resource.Name)
		}
		orders = append(orders, order)
	}
	for i := 1; i < len(orders); i++ {
		if !equalSlice(orders[0], orders[i]) {
			t.Fatalf("non-deterministic order: %v vs %v", orders[0], orders[i])
		}
	}
	// 依赖语义：a-base 在 b-mid 前，b-mid 在 z-app 前。
	idx := map[string]int{}
	for i, n := range orders[0] {
		idx[n] = i
	}
	if !(idx["a-base"] < idx["b-mid"] && idx["b-mid"] < idx["z-app"]) {
		t.Fatalf("dependency order violated: %v", orders[0])
	}
	t.Logf("deterministic execution order: %v", orders[0])
}

func TestDiffEmptyWhenConverged(t *testing.T) {
	d := mustParse(t, `{"resources": [{"name": "a", "kind": "K", "spec": {"x": [1, 2]}}]}`)
	st := store.New()
	if _, err := st.Apply("a", 0, store.Resource{Kind: "K", Spec: map[string]any{"x": []any{1.0, 2.0}}, ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	p, err := Diff(d, st.Snapshot(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if !p.Empty() {
		t.Fatalf("expected empty plan, got %+v", p.Changes)
	}
	t.Logf("converged state yields empty plan (generation=%d)", p.Generation)
}

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
