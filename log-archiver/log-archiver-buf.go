package main

import (
    "fmt"
    "gitee.com/johng/gf/g/container/garray"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/util/gstr"
    "strings"
    "time"
)

// 排序元素项
type bufferItem struct {
    mtime   int64  // 毫秒时间戳
    content string // 日志内容
}

// 添加日志内容到缓冲区
func addToBufferArray(msg *Message) {
    // array是并发安全的
    array := bufferMap.GetOrSetFuncLock(msg.Path, func() interface{} {
        return garray.NewSortedArray(0, func(v1, v2 interface{}) int {
            item1 := v1.(*bufferItem)
            item2 := v2.(*bufferItem)
            // 两个日志项只能排前面或者后面，不能存在相等情况，否则会覆盖
            if item1.mtime < item2.mtime {
                return -1
            } else {
                return 1
            }
        }, true)
    }).(*garray.SortedArray)

    // 判断缓冲区阈值
    for array.Len() > bufferLength {
        glog.Debugfln(`%s exceeds max buffer length: %d > %d, waiting..`, msg.Path, array.Len(), bufferLength)
        time.Sleep(time.Duration(saveInterval)*time.Second)
    }

    for _, v := range msg.Msgs {
        t := getTimeFromContent(v)
        if t == nil || t.IsZero() {
            glog.Debugfln(`cannot parse time from [%s] %s: %s`, msg.Host, msg.Path, v)
            t = gtime.Now()
        }
        array.Add(&bufferItem {
            mtime   : t.Millisecond(),
            content : fmt.Sprintf("%s [%s]\n", strings.TrimRight(v, "\r\n"), msg.Host),
        })
    }
}

// 从内容中解析出日志的时间，并返回对应的日期对象
func getTimeFromContent(content string) *gtime.Time {
    if t := gtime.ParseTimeFromContent(content); t != nil {
        return t
    }
    // 为兼容以时间戳开头的傻逼格式
    // 1540973981 -- s_has_sess -- 50844917 -decryptSess- 50844917__85oxxx
    if len(content) > 10 && gstr.IsNumeric(content[0 : 10]) {
        return gtime.NewFromTimeStamp(gconv.Int64(content[0 : 10]))
    }
    return nil
}
