package main

import (
    "bytes"
    "gitee.com/johng/gf/g/container/garray"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gmlock"
    "gitee.com/johng/gf/g/os/gtime"
    "time"
)

// 异步批量保存日志
func handlerSavingContent() {
    // 批量写日志
    keys := bufferMap.Keys()
    for _, key := range keys {
        go func(path string) {
            // 同时只允许一个该文件的写入任务存在
            if gmlock.TryLock(path) {
                defer gmlock.Unlock(path)
            } else {
                glog.Info(path, "is already saving...")
                return
            }
            array := bufferMap.Get(path).(*garray.SortedArray)
            if array.Len() > 0 {
                minTime := int64(0)
                maxTime := int64(0)
                if array.Len() > 0 {
                    minTime = array.Get(0).(*bufferItem).mtime
                    maxTime = array.Get(array.Len() - 1).(*bufferItem).mtime
                }
                buffer       := bytes.NewBuffer(nil)
                bufferCount  := 0
                tmpOffsetMap := gmap.NewStringIntMap(false)
                for i := 0; i < array.Len(); i++ {
                    item := array.Get(0).(*bufferItem)
                    // 超过缓冲区时间则写入文件
                    if maxTime - minTime >= bufferTime*1000 && bufferCount < bufferLength {
                        // 记录写入的kafka offset
                        key := buildOffsetKey(item.topic, item.partition)
                        if item.offset > tmpOffsetMap.Get(key) {
                            tmpOffsetMap.Set(key, item.offset)
                        }
                        buffer.WriteString(item.content)
                        array.Remove(0)
                        bufferCount++
                    } else {
                        break
                    }
                }
                for buffer.Len() > 0 {
                    if err := gfile.PutBinContentsAppend(path, buffer.Bytes()); err != nil {
                        // 如果日志写失败，等待1秒后继续
                        glog.Error(err)
                        time.Sleep(time.Second)
                    } else {
                        // 真实写入成功之后才写入kafka offset到全局的offset哈希表中，以便磁盘化
                        if tmpOffsetMap.Size() > 0 {
                            for key, offset := range tmpOffsetMap.Clone() {
                                topic, partition := parseOffsetKey(key)
                                if topic != "" {
                                    setOffsetMap(topic, partition, offset)
                                } else {
                                    glog.Error("cannot parse key:", key)
                                }
                            }
                        }
                        glog.Debugfln("%s : %d, %d, %s, %s", path, buffer.Len(), array.Len(),
                            gtime.NewFromTimeStamp(minTime).Format("Y-m-d H:i:s.u"),
                            gtime.NewFromTimeStamp(maxTime).Format("Y-m-d H:i:s.u"),
                        )
                        buffer.Reset()
                        break
                    }
                }
            } else {
                glog.Debugfln("%s empty array", path)
            }
        }(key)
    }
}

// 导出topic offset到磁盘保存
func handlerDumpOffsetMapCron() {
    if !dryrun {
        topicMap.RLockFunc(func(m map[string]interface{}) {
            for _, v := range m {
                go dumpOffsetMap(v.(*gmap.StringIntMap))
            }
        })
    }
}