package shardctrler

import (
	"sort"
)

// 此处将 g2ShardCnt 转化为有序slice。目的：出现负载不能均分的情况时，可以根据这个有序列分配哪些序列应该多分配一个分片，
// 从而避免分片移动过多。
//
//		排序的规则：分片数越多排在前面，分片数相同时，gid大的排在前面
//	 为什么要gid大的排在前面呢？
//	 主要是规定一个顺序出来，当然也可以gid小的排在前面。防止多次运行的配置分配的结果不一样
func g2ShardCntSortSlice(g2ShardCnt map[int]int) []int {
	gids := make([]int, 0, len(g2ShardCnt))
	for gid, _ := range g2ShardCnt {
		gids = append(gids, gid)
	}
	sort.SliceStable(gids, func(i, j int) bool {
		return g2ShardCnt[gids[i]] > g2ShardCnt[gids[j]] ||
			(g2ShardCnt[gids[i]] == g2ShardCnt[gids[j]] && gids[i] > gids[j])
	})
	
	return gids
}

// 假设{1,100,50,10,5,5}的序列应该怎么分配才能尽可能减少分片移动？
// 先排好序{100,50,10,5,5,1}，然后先算平均数（avg）和余数（remainder），每个组都能至少分配avg个分片，
// 排序在前的序列优先分配avg+1，直至分配完remainder
//
//	 算法：
//		 0. 分配之前，先确定好当前gid应该被分配多少的分片（avg,avg+1）
//		 1. 先把负载多的释放
//		 2. 再把负载少的分配
func (sc *ShardCtrler) balance(g2ShardCnt map[int]int, oldShards []int64) []int64 {
	n := len(g2ShardCnt)
	avg := NShards / n
	remainder := NShards % n
	sortedGids := g2ShardCntSortSlice(g2ShardCnt)
	
	// 先把负载多的部分释放
	for gidx := 0; gidx < n; gidx++ {
		target := avg
		if gidx >= n-remainder /*前列应该多分配一个*/ {
			target = avg + 1
		}
		
		// 超出负载
		// 3 3 3 1
		// 3 3 2 1
		if g2ShardCnt[sortedGids[gidx]] > target {
			overLoadGid := sortedGids[gidx]
			changeNum := g2ShardCnt[overLoadGid] - target
			for shard, gid := range oldShards {
				if changeNum <= 0 {
					break
				}
				if gid == int64(overLoadGid) {
					oldShards[shard] = 0
					changeNum--
				}
			}
			g2ShardCnt[overLoadGid] = target
		}
	}
	
	// 为负载少的group分配多出来的group
	for gidx := 0; gidx < n; gidx++ {
		target := avg
		if gidx >= n-remainder {
			target = avg + 1
		}
		
		if g2ShardCnt[sortedGids[gidx]] < target {
			freeGid := sortedGids[gidx]
			changeNum := target - g2ShardCnt[freeGid]
			for shard, gid := range oldShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					oldShards[shard] = int64(freeGid)
					changeNum--
				}
			}
			g2ShardCnt[freeGid] = target
		}
	}
	
	return oldShards
}
