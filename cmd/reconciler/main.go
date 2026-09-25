// 本地演示入口：从 JSON 文件读取期望声明，对内存存储执行收敛并打印结论。
// 用法：go run ./cmd/reconciler <desired.json>
package main

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/reconcile"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/spec"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: reconciler <desired.json>")
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "read:", err)
		os.Exit(1)
	}
	desired, err := spec.Parse(data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse:", err)
		os.Exit(1)
	}

	st := store.New()
	rec := reconcile.New(st, "demo", reconcile.Recompute, 3)

	// 第一次收敛。
	printResult("run 1", rec.Reconcile(context.Background(), desired))
	// 立即重复执行，验证幂等：应全部 noop。
	printResult("run 2 (idempotent re-run)", rec.Reconcile(context.Background(), desired))

	snap := st.Snapshot()
	fmt.Println("== final actual state ==")
	names := make([]string, 0, len(snap.Resources))
	for n := range snap.Resources {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		r := snap.Resources[n]
		fmt.Printf("  %s kind=%s version=%d spec=%v\n", r.Name, r.Kind, r.Version, r.Spec)
	}
}

func printResult(label string, res reconcile.Result) {
	fmt.Printf("== %s ==\n", label)
	fmt.Println("plan (execution order):")
	for i, c := range res.Plan.Changes {
		fmt.Printf("  %d. %-6s %s (expectedVersion=%d)\n", i+1, c.Type, c.Resource.Name, c.ExpectedVersion)
	}
	fmt.Println("statuses:")
	for _, s := range res.Statuses {
		line := fmt.Sprintf("  %-6s %-10s %s", s.Action, s.Result, s.Name)
		if s.Reason != "" {
			line += fmt.Sprintf(" reason=%s", s.Reason)
		}
		if s.Detail != "" {
			line += fmt.Sprintf(" (%s)", s.Detail)
		}
		fmt.Println(line)
	}
	fmt.Printf("converged=%v attempts=%d err=%v\n\n", res.Converged, res.Attempts, res.Err)
}
