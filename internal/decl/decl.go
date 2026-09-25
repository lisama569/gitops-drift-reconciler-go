// Package decl 定义期望状态声明的解析与校验。
// 校验规则：任何不合法、互相矛盾或引用不存在依赖的声明
// 必须整体被拒绝，不允许按部分内容继续执行。
package decl

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Resource 描述一个期望资源。
type Resource struct {
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	DependsOn []string          `json:"dependsOn,omitempty"`
	Spec      map[string]string `json:"spec,omitempty"`
}

// Key 返回资源的唯一标识 kind/name。
func (r Resource) Key() string { return r.Kind + "/" + r.Name }

// Declaration 是一份期望状态声明。
type Declaration struct {
	Resources []Resource `json:"resources"`
}

// Parse 从 JSON 字节解析声明，解析失败返回错误。
func Parse(data []byte) (*Declaration, error) {
	var d Declaration
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("decl: 解析声明失败: %w", err)
	}
	return &d, nil
}

// Validate 校验声明，发现以下任一问题时整体拒绝：
//   - 资源缺少 kind 或 name
//   - 资源 key 重复（互相矛盾）
//   - dependsOn 引用不存在的资源
//   - 依赖关系存在环
func Validate(d *Declaration) error {
	if d == nil {
		return fmt.Errorf("decl: 声明为空")
	}
	seen := make(map[string]Resource, len(d.Resources))
	for _, r := range d.Resources {
		if strings.TrimSpace(r.Kind) == "" {
			return fmt.Errorf("decl: 资源 %q 缺少 kind", r.Name)
		}
		if strings.TrimSpace(r.Name) == "" {
			return fmt.Errorf("decl: kind %q 下存在缺少 name 的资源", r.Kind)
		}
		key := r.Key()
		if _, dup := seen[key]; dup {
			return fmt.Errorf("decl: 资源 %s 重复声明", key)
		}
		seen[key] = r
	}
	for _, r := range d.Resources {
		for _, dep := range r.DependsOn {
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("decl: 资源 %s 依赖不存在的资源 %s", r.Key(), dep)
			}
			if dep == r.Key() {
				return fmt.Errorf("decl: 资源 %s 依赖自身", r.Key())
			}
		}
	}
	if _, err := TopoOrder(d); err != nil {
		return err
	}
	return nil
}

// TopoOrder 返回按依赖关系拓扑排序后的资源列表（依赖在前）。
// 排序在同等条件下按 key 字典序，保证结果稳定可复现。
// 存在环时返回错误。
func TopoOrder(d *Declaration) ([]Resource, error) {
	byKey := make(map[string]Resource, len(d.Resources))
	indeg := make(map[string]int, len(d.Resources))
	children := make(map[string][]string, len(d.Resources))
	for _, r := range d.Resources {
		byKey[r.Key()] = r
		indeg[r.Key()] = len(r.DependsOn)
		for _, dep := range r.DependsOn {
			children[dep] = append(children[dep], r.Key())
		}
	}
	// 就绪队列按 key 排序，保证确定性。
	var ready []string
	for k, n := range indeg {
		if n == 0 {
			ready = append(ready, k)
		}
	}
	sort.Strings(ready)

	var order []Resource
	for len(ready) > 0 {
		k := ready[0]
		ready = ready[1:]
		order = append(order, byKey[k])
		var next []string
		for _, c := range children[k] {
			indeg[c]--
			if indeg[c] == 0 {
				next = append(next, c)
			}
		}
		if len(next) > 0 {
			sort.Strings(next)
			ready = append(ready, next...)
			sort.Strings(ready)
		}
	}
	if len(order) != len(d.Resources) {
		var inCycle []string
		for k, n := range indeg {
			if n > 0 {
				inCycle = append(inCycle, k)
			}
		}
		sort.Strings(inCycle)
		return nil, fmt.Errorf("decl: 依赖关系存在环，涉及资源: %s", strings.Join(inCycle, ", "))
	}
	return order, nil
}
