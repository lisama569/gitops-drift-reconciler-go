package plan

import (
	"testing"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
)

func desired() *decl.Declaration {
	return &decl.Declaration{Resources: []decl.Resource{
		{Kind: "App", Name: "web", DependsOn: []string{"App/db"}, Spec: map[string]string{"image": "web:v2"}},
		{Kind: "App", Name: "db", Spec: map[string]string{"image": "db:v1"}},
		{Kind: "App", Name: "cache", Spec: map[string]string{"image": "cache:v1"}},
	}}
}

func TestComputeClassifiesChanges(t *testing.T) {
	actual := map[string]decl.Resource{
		// web 存在但字段漂移
		"App/web": {Kind: "App", Name: "web", Spec: map[string]string{"image": "web:v1"}},
		// db 缺失 -> create
		// cache 与期望一致 -> 无变更
		"App/cache": {Kind: "App", Name: "cache", Spec: map[string]string{"image": "cache:v1"}},
		// orphan 多余 -> delete
		"App/orphan": {Kind: "App", Name: "orphan", Spec: map[string]string{"x": "y"}},
	}
	p, err := Compute(desired(), actual, 7)
	if err != nil {
		t.Fatalf("计算计划失败: %v", err)
	}
	t.Logf("变更计划:\n%s", p)
	if p.BaseVersion != 7 {
		t.Fatalf("baseVersion 不符: %d", p.BaseVersion)
	}
	if len(p.Steps) != 3 {
		t.Fatalf("应有 3 个步骤: %v", p.Steps)
	}
	// create(db) 必须先于 update(web)，delete 在最后。
	if p.Steps[0].Kind != Create || p.Steps[0].Resource.Key() != "App/db" {
		t.Fatalf("第一步应为 create App/db: %+v", p.Steps[0])
	}
	if p.Steps[1].Kind != Update || p.Steps[1].Resource.Key() != "App/web" {
		t.Fatalf("第二步应为 update App/web: %+v", p.Steps[1])
	}
	if p.Steps[2].Kind != Delete || p.Steps[2].Resource.Key() != "App/orphan" {
		t.Fatalf("第三步应为 delete App/orphan: %+v", p.Steps[2])
	}
	t.Logf("判定依据: create=%q update=%q delete=%q",
		p.Steps[0].Reason, p.Steps[1].Reason, p.Steps[2].Reason)
}

func TestComputeReproducible(t *testing.T) {
	actual := map[string]decl.Resource{
		"App/web": {Kind: "App", Name: "web", Spec: map[string]string{"image": "web:v1"}},
	}
	base, err := Compute(desired(), actual, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, err := Compute(desired(), actual, 3)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != base.String() {
			t.Fatalf("计划不可复现:\n%s\nvs\n%s", got, base)
		}
	}
}

func TestComputeRejectsInvalidDeclaration(t *testing.T) {
	d := &decl.Declaration{Resources: []decl.Resource{
		{Kind: "App", Name: "a", DependsOn: []string{"App/ghost"}},
	}}
	if _, err := Compute(d, nil, 0); err == nil {
		t.Fatal("引用不存在依赖的声明应被拒绝")
	} else {
		t.Logf("拒绝依据: %v", err)
	}
}

func TestComputeEmptyWhenConverged(t *testing.T) {
	d := desired()
	actual := map[string]decl.Resource{}
	for _, r := range d.Resources {
		actual[r.Key()] = r
	}
	p, err := Compute(d, actual, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Empty() {
		t.Fatalf("已收敛时不应有变更: %s", p)
	}
	t.Log("已收敛，计划为空，符合幂等预期")
}
