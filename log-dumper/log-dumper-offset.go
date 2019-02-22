package main

import (
    "fmt"
    "github.com/gogf/gf/g/container/gmap"
    "github.com/gogf/gf/g/os/gfile"
    "github.com/gogf/gf/g/util/gconv"
    "github.com/gogf/gf/g/text/gregex"
)

// 生成kafka消费offset文件路径
func offsetFilePath(offsetKey string) string {
    return fmt.Sprintf("%s/%s/%s.offset", logPath, KAFKA_OFFSETS_DIR_NAME, offsetKey)
}

// 初始化topic offset
func initOffsetMap(topic string, offsetMap *gmap.StringIntMap) {
    for i := 0; i < 100; i++ {
        key  := buildOffsetKey(topic, i)
        path := offsetFilePath(key)
        if !gfile.Exists(path) {
            break
        }
        offsetMap.Set(key, gconv.Int(gfile.GetContents(path)))
    }
}

// 根据topic和partition生成key
func buildOffsetKey(topic string, partition int) string {
    return fmt.Sprintf("%s.%d", topic, partition)
}

// 从key中解析出topic和partition，是buildOffsetKey的相反操作
func parseOffsetKey(key string) (topic string, partition int) {
    match, _ := gregex.MatchString(`(.+)\.(\d+)`, key)
    if len(match) > 0 {
        return match[1], gconv.Int(match[2])
    }
    return "", 0
}

// 设置topic offset
func setOffsetMap(topic string, partition int, offset int) {
    key := buildOffsetKey(topic, partition)
    topicMap.RLockFunc(func(m map[string]interface{}) {
        if r, ok := m[topic]; ok {
            r.(*gmap.StringIntMap).Set(key, offset)
        }
    })
}

// 应用自定义保存当前kafka读取的offset
func dumpOffsetMap(offsetMap *gmap.StringIntMap) {
    if dryrun || offsetMap.Size() == 0 {
        return
    }
    offsetMap.RLockFunc(func(m map[string]int) {
        for key, offset := range m {
            if offset == 0 {
                continue
            }
            gfile.PutContents(offsetFilePath(key), gconv.String(offset))
        }
    })
}
