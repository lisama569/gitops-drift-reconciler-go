// Package spec 负责期望声明（DesiredState）的解析与校验。
// 不合法、自相矛盾或引用不存在依赖的声明必须在此阶段被整体拒绝，
// 不允许按其中一部分内容继续执行。
package spec

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalid 是声明校验失败的根错误，可用 errors.Is 判定。
var ErrInvalid = errors.New("spec: invalid desired state")

// Resource 是期望声明中的单个资源。
type Resource struct {
	Name      string         `json:"name"`
	Kind      string         `json:"kind"`
	DependsOn []string       `json:"dependsOn,omitempty"`
	Spec      map[string]any `json:"spec,omitempty"`
}

// DesiredState 是一份完整的期望声明。
type DesiredState struct {
	Resources []Resource `json:"resources"`
}

// Parse 从 JSON 字节解析期望声明，并执行完整校验。
// 解析失败或校验失败都会返回包裹 ErrInvalid 的错误。
func Parse(data []byte) (DesiredState, error) {
	var d DesiredState
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return DesiredState{}, fmt.Errorf("%w: malformed JSON: %v", ErrInvalid, err)
	}
	if err := Validate(d); err != nil {
		return DesiredState{}, err
	}
	return d, nil
}

// Validate 校验声明，任何一项不满足都整体拒绝：
//   - 至少包含一个资源；
//   - 名称/类型非空，名称唯一（重名即自相矛盾）；
//   - 依赖必须引用同一份声明中存在的资源（不允许悬空依赖）；
//   - 依赖图无环。
func Validate(d DesiredState) error {
	if len(d.Resources) == 0 {
		return fmt.Errorf("%w: desired state must contain at least one resource", ErrInvalid)
	}

	byName := make(map[string]Resource, len(d.Resources))
	for _, r := range d.Resources {
		if strings.TrimSpace(r.Name) == "" {
			return fmt.Errorf("%w: resource with empty name", ErrInvalid)
		}
		if strings.TrimSpace(r.Kind) == "" {
			return fmt.Errorf("%w: resource %q has empty kind", ErrInvalid, r.Name)
		}
		if _, dup := byName[r.Name]; dup {
			return fmt.Errorf("%w: duplicate resource name %q", ErrInvalid, r.Name)
		}
		byName[r.Name] = r
	}

	for _, r := range d.Resources {
		seen := map[string]bool{}
		for _, dep := range r.DependsOn {
			if dep == r.Name {
				return fmt.Errorf("%w: resource %q depends on itself", ErrInvalid, r.Name)
			}
			if seen[dep] {
				return fmt.Errorf("%w: resource %q lists dependency %q twice", ErrInvalid, r.Name, dep)
			}
			seen[dep] = true
			if _, ok := byName[dep]; !ok {
				return fmt.Errorf("%w: resource %q depends on unknown resource %q", ErrInvalid, r.Name, dep)
			}
		}
	}

	if err := checkAcyclic(d.Resources); err != nil {
		return err
	}
	return nil
}

// checkAcyclic 用三色标记法检测依赖环。
func checkAcyclic(resources []Resource) error {
	const (
		white = 0 // 未访问
		gray  = 1 // 访问中（在当前 DFS 栈上）
		black = 2 // 已完成
	)
	deps := make(map[string][]string, len(resources))
	color := make(map[string]int, len(resources))
	for _, r := range resources {
		deps[r.Name] = r.DependsOn
	}

	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		switch color[name] {
		case gray:
			cycle := append(append([]string{}, path...), name)
			return fmt.Errorf("%w: dependency cycle detected: %s", ErrInvalid, strings.Join(cycle, " -> "))
		case black:
			return nil
		}
		color[name] = gray
		// 排序保证报错信息确定、可复现。
		sorted := append([]string{}, deps[name]...)
		sort.Strings(sorted)
		for _, dep := range sorted {
			if err := visit(dep, append(path, name)); err != nil {
				return err
			}
		}
		color[name] = black
		return nil
	}

	names := make([]string, 0, len(deps))
	for n := range deps {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := visit(n, nil); err != nil {
			return err
		}
	}
	return nil
}
