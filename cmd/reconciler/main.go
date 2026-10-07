// 本地演示：
//  1. 单实例连续两次收敛，第二次应为空计划（幂等）；
//  2. 多个独立执行者并发收敛同一份期望与实际状态（replan 策略），
//     每个资源全程只被成功下发一次，所有实例收敛到同一结论。
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

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

	fmt.Println("--- 单实例幂等演示 ---")
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

	fmt.Println("--- 多实例并发演示（4 个独立执行者，replan 策略） ---")
	shared := store.New()
	const instances = 4
	var wg sync.WaitGroup
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := reconcile.New(shared).RunWithOptions(
				context.Background(), d, reconcile.Options{DriftPolicy: reconcile.DriftReplan})
			if err != nil {
				log.Printf("实例 %d 收敛失败: %v", id, err)
				return
			}
			fmt.Printf("实例 %d: applied=%d replans=%d converged=%v\n",
				id, len(res.Applied), res.Replans, res.Converged())
		}(i)
	}
	wg.Wait()
	_, ver := shared.Snapshot()
	fmt.Printf("最终状态: keys=%v 成功写入总数=%d（每个资源恰好一次）\n", shared.Keys(), ver)
}
