// 本地演示：解析期望声明 -> 计算计划 -> 执行收敛 -> 重复执行验证幂等。
package main

import (
	"fmt"
	"log"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/reconcile"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

const manifest = `{
  "resources": [
    {"kind": "ConfigMap", "name": "app-config", "spec": {"log.level": "info"}},
    {"kind": "Deployment", "name": "app", "dependsOn": ["ConfigMap/app-config"],
     "spec": {"image": "app:v1", "replicas": "2"}}
  ]
}`

func main() {
	d, err := decl.Parse([]byte(manifest))
	if err != nil {
		log.Fatalf("解析声明失败: %v", err)
	}
	s := store.New()
	r := reconcile.New(s)

	for i := 1; i <= 2; i++ {
		res, err := r.Run(d)
		if err != nil {
			log.Fatalf("收敛失败: %v", err)
		}
		fmt.Printf("=== 第 %d 次收敛 ===\n%s", i, res)
		fmt.Printf("状态快照: %v\n\n", s.Keys())
	}
}
