package main

import (
    "fmt"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/util/gconv"
)

// 初始化topic offset
func initOffsetMap(topic string, offsetMap *gmap.StringIntMap) {
    for i := 0; i < 100; i++ {
        key  := fmt.Sprintf("%s.%d", topic, i)
        path := fmt.Sprintf("%s/%s/%s", logPath, KAFKA_OFFSETS_DIR_NAME, key)
        if !gfile.Exists(path) {
            break
        }
        offsetMap.Set(key, gconv.Int(gfile.GetContents(path)))
    }
}

// 应用自定义保存当前kafka读取的offset
func dumpOffsetMap(offsetMap *gmap.StringIntMap) {
    offsetMap.RLockFunc(func(m map[string]int) {
        for k, v := range m {
            if v == 0 {
                continue
            }
            path    := fmt.Sprintf("%s/%s/%s", logPath, KAFKA_OFFSETS_DIR_NAME, k)
            content := gconv.String(v)
            gfile.PutContents(path, content)
        }
    })
}
