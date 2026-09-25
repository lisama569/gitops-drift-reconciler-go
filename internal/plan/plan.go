// Package plan 计算期望与实际状态的差异并生成有序变更计划。
// 同一份期望与实际状态的组合必然得到相同的计划与执行顺序：
//   - create/update 步骤按依赖拓扑序排列（依赖在前），同层按 key 字典序；
//   - delete 步骤（多余资源）排在最后，按 key 字典序。
package plan

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
)

// ChangeKind 区分变更类型。
type ChangeKind string

const (
	Create ChangeKind = "create"
	Update ChangeKind = "update"
	Delete ChangeKind = "delete"
)

// Step 是计划中的一个变更步骤。
type Step struct {
	Kind     ChangeKind
	Resource decl.Resource
	Reason   string // 人类可读的判定依据
}

// Plan 是有序的变更计划。
type Plan struct {
	Steps       []Step
	BaseVersion uint64 // 计划生成时观察到的实际状态全局版本
}

// Empty 报告计划是否无变更。
func (p *Plan) Empty() bool { return len(p.Steps) == 0 }

// String 以稳定格式输出计划，便于日志与测试比对。
func (p *Plan) String() string {
	if p.Empty() {
		return "plan: (无变更)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "plan: baseVersion=%d\n", p.BaseVersion)
	for i, s := range p.Steps {
		fmt.Fprintf(&b, "  %d. %s %s (%s)\n", i+1, s.Kind, s.Resource.Key(), s.Reason)
	}
	return b.String()
}

// Compute 计算差异并生成拓扑有序的变更计划。
// actual 为实际状态快照（key -> 资源），baseVersion 为快照版本。
func Compute(d *decl.Declaration, actual map[string]decl.Resource, baseVersion uint64) (*Plan, error) {
	if err := decl.Validate(d); err != nil {
		return nil, err
	}
	order, err := decl.TopoOrder(d)
	if err != nil {
		return nil, err
	}
	p := &Plan{BaseVersion: baseVersion}
	desired := make(map[string]struct{}, len(order))
	for _, r := range order {
		key := r.Key()
		desired[key] = struct{}{}
		cur, exists := actual[key]
		switch {
		case !exists:
			p.Steps = append(p.Steps, Step{Kind: Create, Resource: r, Reason: "资源缺失"})
		default:
			if reason := specDrift(cur.Spec, r.Spec); reason != "" {
				p.Steps = append(p.Steps, Step{Kind: Update, Resource: r, Reason: "字段漂移: " + reason})
			}
		}
	}
	// 多余资源：实际存在但期望中不存在，排在最后，按 key 排序保证确定性。
	var extra []string
	for key := range actual {
		if _, ok := desired[key]; !ok {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	for _, key := range extra {
		r := actual[key]
		p.Steps = append(p.Steps, Step{Kind: Delete, Resource: r, Reason: "多余资源，期望声明中不存在"})
	}
	return p, nil
}

// specDrift 比较两个 spec，返回逐字段差异描述（按字段名排序）；
// 无差异时返回空串。
func specDrift(actual, desired map[string]string) string {
	keys := map[string]struct{}{}
	for k := range actual {
		keys[k] = struct{}{}
	}
	for k := range desired {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var diffs []string
	for _, k := range sorted {
		a, inA := actual[k]
		des, inD := desired[k]
		switch {
		case !inA:
			diffs = append(diffs, fmt.Sprintf("%s: <缺失> -> %q", k, des))
		case !inD:
			diffs = append(diffs, fmt.Sprintf("%s: %q -> <移除>", k, a))
		case a != des:
			diffs = append(diffs, fmt.Sprintf("%s: %q -> %q", k, a, des))
		}
	}
	return strings.Join(diffs, "; ")
}
