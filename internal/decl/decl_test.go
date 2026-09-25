package decl

import (
	"strings"
	"testing"
)

func TestParseValid(t *testing.T) {
	d, err := Parse([]byte(`{"resources":[{"kind":"A","name":"a","spec":{"x":"1"}}]}`))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if len(d.Resources) != 1 || d.Resources[0].Key() != "A/a" {
		t.Fatalf("解析结果不符: %+v", d.Resources)
	}
}

func TestParseInvalidJSON(t *testing.T) {
	if _, err := Parse([]byte(`{not json`)); err == nil {
		t.Fatal("非法 JSON 应被拒绝")
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]Declaration{
		"缺少name":      {Resources: []Resource{{Kind: "A"}}},
		"缺少kind":      {Resources: []Resource{{Name: "a"}}},
		"重复声明":       {Resources: []Resource{{Kind: "A", Name: "a"}, {Kind: "A", Name: "a"}}},
		"依赖不存在":     {Resources: []Resource{{Kind: "A", Name: "a", DependsOn: []string{"B/b"}}}},
		"依赖自身":       {Resources: []Resource{{Kind: "A", Name: "a", DependsOn: []string{"A/a"}}}},
		"依赖环": {Resources: []Resource{
			{Kind: "A", Name: "a", DependsOn: []string{"A/b"}},
			{Kind: "A", Name: "b", DependsOn: []string{"A/a"}},
		}},
	}
	for name, d := range cases {
		d := d
		t.Run(name, func(t *testing.T) {
			if err := Validate(&d); err == nil {
				t.Fatalf("声明应被整体拒绝: %+v", d)
			} else {
				t.Logf("拒绝依据: %v", err)
			}
		})
	}
}

func TestTopoOrderDeterministic(t *testing.T) {
	d := &Declaration{Resources: []Resource{
		{Kind: "App", Name: "web", DependsOn: []string{"App/db", "App/cache"}},
		{Kind: "App", Name: "db"},
		{Kind: "App", Name: "cache"},
		{Kind: "App", Name: "worker", DependsOn: []string{"App/db"}},
	}}
	first, err := TopoOrder(d)
	if err != nil {
		t.Fatalf("拓扑排序失败: %v", err)
	}
	for i := 0; i < 20; i++ {
		got, err := TopoOrder(d)
		if err != nil {
			t.Fatalf("拓扑排序失败: %v", err)
		}
		if strings.Join(keys(got), ",") != strings.Join(keys(first), ",") {
			t.Fatalf("拓扑顺序不稳定: %v vs %v", keys(got), keys(first))
		}
	}
	t.Logf("依赖顺序: %v", keys(first))
	// 依赖必须先于使用者出现。
	pos := map[string]int{}
	for i, r := range first {
		pos[r.Key()] = i
	}
	for _, r := range d.Resources {
		for _, dep := range r.DependsOn {
			if pos[dep] > pos[r.Key()] {
				t.Fatalf("依赖 %s 未先于 %s", dep, r.Key())
			}
		}
	}
}

func keys(rs []Resource) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Key()
	}
	return out
}
