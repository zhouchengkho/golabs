package shardmaster

import (
	// "fmt"
	"math/big"
	"crypto/rand"
	"time"
	// "fmt"
)

const (
	SLEEPTIME = 10 * time.Millisecond
	MAXSLEEP = 5 * time.Second
)

func genOp(t string, args Args) Op {
	return Op{Type: t, Args: args, Uid: nrand()}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func copyMap(m map[int64][]string) map[int64][]string {
	newMap := make(map[int64][]string)
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

// join leave move query

func groupAfterJoin(configs []Config, gid int64, servers []string) map[int64][]string{
	groups := copyMap(configs[len(configs) - 1].Groups)
	groups[gid] = servers
	return groups
}

func shardsAfterJoin(groups map[int64][]string, originShards [NShards]int64) [NShards]int64 {

	now := getOriginalLoad(originShards, groups)
	target := getTargetLoad(originShards, groups)
	// fmt.Printf("origin: %v , target: %v, shards: %v \n", now, target, originShards)

	for gid, targetCount := range target {
		nowCount, exist := now[gid]
		if exist && nowCount > targetCount {
			for now[gid] > targetCount {
				findGIDAndRemove(gid, &originShards)
				now[gid] = now[gid] - 1
			}
		}
	}

	for gid := range now {
		_, exist := target[gid]
		if !exist {
			findAllGIDAndRemove(gid, &originShards)
			delete(now, gid)
		}
	}

	for len(target) > 0 {
		for gid, targetCount := range target {
			nowCount, exist := now[gid]
			if exist {
				// fmt.Printf("ss: gid %v, target count %v, now count %v \n", gid, targetCount, nowCount)

				if nowCount == targetCount {
					delete(target, gid)
				} else if nowCount < targetCount {
					findEmptyAndApply(gid, &originShards)
					target[gid] = target[gid] - 1
				}
			} else {
				if targetCount == 0 {
					delete(target, gid)
				} else {
					findEmptyAndApply(gid, &originShards)
					target[gid] = target[gid] - 1
				}
			}
		}
	}

	return originShards
}

func groupAfterLeave(configs []Config, gid int64) map[int64][]string {
	groups := copyMap(configs[len(configs) - 1].Groups)
	delete(groups, gid)
	return groups
}

func shardsAfterLeave(groups map[int64][]string, shards [NShards]int64) [NShards]int64 {
	return shardsAfterJoin(groups, shards)
}

func groupAfterMove(configs []Config) map[int64][]string {
	return copyMap(configs[len(configs) - 1].Groups)
}

func shardsAfterMove(configs []Config, num int, gid int64) [NShards]int64 {
	shards := configs[len(configs) - 1].Shards
	shards[num] = gid
	return shards
}

func createConfigs(configs []Config) []Config {
	newConfig := make([]Config, len(configs) + 1)
	for i := 0; i < len(configs); i++ {
		newConfig[i] = configs[i]
	}
	return newConfig
}

func getOriginalLoad(shards [NShards]int64, groups map[int64][]string) map[int64]int {
	m := make(map[int64]int)
	for _, gid := range shards {
		if gid == 0 {
			continue
		}
		count, exist := m[gid]
		if !exist {
			m[gid] = 1
		} else {
			m[gid] = count + 1
		}
	}
	return m
}

func getTargetLoad(shards [NShards]int64, groups map[int64][]string) map[int64]int {
	m := make(map[int64]int)
	if len(groups) == 0 {
		return m
	}
	each := NShards/len(groups)
	hasLeft := NShards%len(groups) != 0
	if len(groups) >= NShards {
		each = 1
	}
	count := 0
	for gid := range groups {
		if gid == 0 {
			continue
		}
		if hasLeft {
			m[gid] = each + 1
			hasLeft = true
		}
		m[gid] = each
		count++
		if count == NShards {
			break
		}
	}
	return m
}

func findGIDAndRemove(gid int64, shards *[NShards]int64) {
	for i, v := range shards {
		if v == gid {
			shards[i] = 0
			return
		}
	}
}

func findEmptyAndApply(gid int64, shards *[NShards]int64) {
	for i, v := range shards {
		if v == 0 {
			shards[i] = gid
			return
		}
	}
}

func findAllGIDAndRemove(gid int64, shards *[NShards]int64) {
	for i, v := range shards {
		if v == gid {
			shards[i] = 0
		}
	}
}



func (c *Config) copy() Config {
	var config = Config{Num: c.Num,
		Shards: c.Shards,
		Groups: make(map[int64][]string)}
	for key, value := range c.Groups {
		config.Groups[key] = value
	}
	return config
}

func (c *Config) addGroup(gid int64, servers []string) {
	c.Num += 1
	c.Groups[gid] = servers
}

func (c *Config) removeGroupByGID(gid int64) {
	c.Num += 1
	delete(c.Groups, gid)
}


func (c *Config) move(i int, gid int64) {
	c.Shards[i] = gid
}


func (c *Config) getLoads() map[int64]int {
	loads := make(map[int64]int)
	for gid, _ := range c.Groups {
		loads[gid] = 0                  // valid replica group gids are in the Groups map
	}
	for _, gid := range c.Shards {
		_, present := loads[gid]        // Only increment for valid gids (i.e. in the table)
		if present {
			loads[gid] += 1
		}
	}
	return loads
}


func (c *Config) minLoad() (gid int64, load int) {
	loads := c.getLoads()
	var minGID int64 = 0        // repica gids are int64 (o is default non-valid gid)
	var minLoad = NShards + 1        // bc 1 real RG w/ all shards preferable replica group 0

	for gid, shardCount := range loads {
		if shardCount < minLoad {
			minLoad = shardCount
			minGID = gid
		}
	}
	return minGID, minLoad
}


func (c *Config) maxLoad() (gid int64, load int) {
	loads := c.getLoads()
	var maxGID int64 = 0     // repica gids are int64 (o is default non-valid gid)
	var maxLoad = 0               // non-valid RG 0 is not actually responsible for shards.

	for gid, shardCount := range loads {
		if shardCount > maxLoad {
			maxLoad = shardCount
			maxGID = gid
		}
	}
	return maxGID, maxLoad
}


func (c *Config) loadDiff() int {
	_, min := c.minLoad()
	_, max := c.maxLoad()
	return max - min
}


func (c *Config) occupyInvalid() {
	for i, gid := range c.Shards {
		if gid == 0 {                // non-valid RG, used before a Join has occurred.
			min, _ := c.minLoad()
			c.move(i, min)
		}
	}
	return
}


func (c *Config) moveAround(targetGID int64) {
	for i, gid := range c.Shards {
		if gid == targetGID {
			min, _ := c.minLoad()
			c.move(i, min)
		}
	}
}


func (c *Config) checkLoad(threshold int) {
	if threshold < 1 {
		return
	}
	var max, min int64
	for c.loadDiff() > threshold {
		max, _ = c.maxLoad()
		min, _ = c.minLoad()
		for i, gid := range c.Shards {
			if gid == max {
				c.move(i, min)
				break
			}
		}
	}
}

// end of join leave move query

