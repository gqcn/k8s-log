package main

import (
    "fmt"
    "gitee.com/johng/gf/g/container/garray"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gmlock"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gregex"
    "strings"
)

// 排序元素项
type bufferItem struct {
    mtime   int64  // 毫秒时间戳
    content string // 日志内容
}

// 添加日志内容到缓冲区
func addToBufferArray(msg *Message) {
    // 使用内存锁保证同一时刻只有一个goroutine在操作buffer
    gmlock.Lock(msg.Path)
    array := bufferMap.GetOrSetFuncLock(msg.Path, func() interface{} {
        return garray.NewSortedArray(0, func(v1, v2 interface{}) int {
            item1 := v1.(*bufferItem)
            item2 := v2.(*bufferItem)
            // 两个元素项只能排前面或者后面，不存在相等情况
            if item1.mtime < item2.mtime {
                return -1
            } else {
                return 1
            }
        }, false)
    }).(*garray.SortedArray)
    for _, v := range msg.Msgs {
        t := getTimeFromContent(v)
        if t.IsZero() {
            t = gtime.Now()
        }
        array.Add(&bufferItem {
            mtime   : t.Millisecond(),
            content : fmt.Sprintf("%s [%s]\n", strings.TrimRight(v, "\r\n"), msg.Host),
        })
    }
    gmlock.Unlock(msg.Path)
}

// 从内容中解析出日志的时间，并返回对应的日期对象
func getTimeFromContent(content string) *gtime.Time {
    match, _ := gregex.MatchString(gtime.TIME_REAGEX_PATTERN, content)
    if len(match) >= 1 {
        return gtime.NewFromStr(match[0])
    } else {
        glog.Warningfln(`cannot get time str from log: %s`, content)
    }
    return nil
}
