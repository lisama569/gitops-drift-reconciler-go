// Package plan 负责期望状态与实际状态的差异计算，
// 产出确定、可复现的变更计划及依赖顺序。
//
// 排序语义：先按依赖关系做拓扑排序（被依赖者在前），
// 同一层内按资源名称字典序排列；删除操作排在所有创建/更新之后，
// 且按依赖的反序执行（先删依赖者，再删被依赖者）。
package plan

import (
	"fmt"
	"sort"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/spec"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

// ChangeType 区分变更类别。
type ChangeType string

const (
	Create ChangeType = "create" // 资源缺失
	Update ChangeType = "update" // 字段漂移
	Delete ChangeType = "delete" // 多余资源
	NoOp   ChangeType = "noop"   // 已一致
)

// Change 是计划中的单个变更项。
type Change struct {
	Type            ChangeType
	Resource        spec.Resource
	ExpectedVersion uint64 // 计划生成时观察到的实际版本，用于执行期 CAS；0 表示要求不存在
}

// Plan 是按执行顺序排列的变更列表。
type Plan struct {
	Changes    []Change
	Generation uint64 // 计划所基于的实际状态代际
}

// Empty 报告计划是否不含任何实际变更（create/update/delete）。
func (p Plan) Empty() bool {
	for _, c := range p.Changes {
		if c.Type != NoOp {
			return false
		}
	}
	return true
}

// Diff 计算期望声明与快照之间的差异，返回确定性排序的计划。
// 同一 (desired, snapshot) 组合必然产出相同的计划与顺序。
// managedBy 用于识别“多余资源”：实际状态中存在、由本控制器管理、
// 但不在期望声明中的资源将被计划删除。
func Diff(d spec.DesiredState, snap store.Snapshot, managedBy string) (Plan, error) {
	if err := spec.Validate(d); err != nil {
		return Plan{}, err
	}

	desired := make(map[string]spec.Resource, len(d.Resources))
	for _, r := range d.Resources {
		desired[r.Name] = r
	}

	upserts := make(map[string]Change, len(d.Resources))
	for _, r := range d.Resources {
		cur, exists := snap.Resources[r.Name]
		switch {
		case !exists:
			upserts[r.Name] = Change{Type: Create, Resource: r, ExpectedVersion: 0}
		case drifted(cur, r):
			upserts[r.Name] = Change{Type: Update, Resource: r, ExpectedVersion: cur.Version}
		default:
			upserts[r.Name] = Change{Type: NoOp, Resource: r, ExpectedVersion: cur.Version}
		}
	}

	// 多余资源：由本控制器管理但不在期望声明中。
	var extras []store.Resource
	for _, cur := range snap.Resources {
		if cur.ManagedBy == managedBy {
			if _, wanted := desired[cur.Name]; !wanted {
				extras = append(extras, cur)
			}
		}
	}

	order, err := topoOrder(d.Resources)
	if err != nil {
		return Plan{}, err
	}

	changes := make([]Change, 0, len(order)+len(extras))
	for _, name := range order {
		changes = append(changes, upserts[name])
	}

	// 删除按期望依赖图的反拓扑序：先删“更下游”的多余资源。
	// 多余资源不在期望图中，彼此之间无声明依赖，按名称字典序保证确定性。
	sort.Slice(extras, func(i, j int) bool { return extras[i].Name < extras[j].Name })
	for _, e := range extras {
		changes = append(changes, Change{
			Type: Delete,
			Resource: spec.Resource{
				Name: e.Name,
				Kind: e.Kind,
			},
			ExpectedVersion: e.Version,
		})
	}

	return Plan{Changes: changes, Generation: snap.Generation}, nil
}

// drifted 判定实际资源是否与期望存在字段漂移（kind 或 spec 不一致）。
func drifted(cur store.Resource, want spec.Resource) bool {
	if cur.Kind != want.Kind {
		return true
	}
	wantSpec := want.Spec
	if wantSpec == nil {
		wantSpec = map[string]any{}
	}
	curSpec := cur.Spec
	if curSpec == nil {
		curSpec = map[string]any{}
	}
	return !equalJSON(wantSpec, curSpec)
}

// topoOrder 返回确定性拓扑序：Kahn 算法，就绪集合按名称字典序取最小者。
func topoOrder(resources []spec.Resource) ([]string, error) {
	indegree := make(map[string]int, len(resources))
	dependents := make(map[string][]string, len(resources))
	for _, r := range resources {
		indegree[r.Name] = len(r.DependsOn)
		for _, dep := range r.DependsOn {
			dependents[dep] = append(dependents[dep], r.Name)
		}
	}

	var ready []string
	for name, deg := range indegree {
		if deg == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(resources))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		next := dependents[name]
		sort.Strings(next)
		for _, m := range next {
			indegree[m]--
			if indegree[m] == 0 {
				// 插入并保持 ready 有序。
				i := sort.SearchStrings(ready, m)
				ready = append(ready, "")
				copy(ready[i+1:], ready[i:])
				ready[i] = m
			}
		}
	}
	if len(order) != len(resources) {
		return nil, fmt.Errorf("plan: dependency cycle among %d resources", len(resources)-len(order))
	}
	return order, nil
}

// equalJSON 比较两个 JSON 风格值（标量/切片/映射）。
func equalJSON(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, ok := bv[k]
			if !ok || !equalJSON(v, bvv) {
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
			if !equalJSON(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
