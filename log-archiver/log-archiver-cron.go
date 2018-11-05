package main

import (
    "bytes"
    "fmt"
    "gitee.com/johng/gf/g/container/garray"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gmlock"
    "gitee.com/johng/gf/g/os/gproc"
    "gitee.com/johng/gf/g/os/gtime"
    "os"
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
                tmpOffsetMap := gmap.NewStringIntMap(false)
                for i := 0; i < array.Len(); i++ {
                    item := array.Get(0).(*bufferItem)
                    // 超过缓冲区时间则写入文件
                    if maxTime - minTime >= bufferTime*1000 {
                        // 记录写入的kafka offset
                        key := buildOffsetKey(item.topic, item.partition)
                        if item.offset > tmpOffsetMap.Get(key) {
                            tmpOffsetMap.Set(key, item.offset)
                        }
                        buffer.WriteString(item.content)
                        array.Remove(0)
                        if dryrun {
                            glog.Debugfln(`%s item: %s`, path, gtime.NewFromTimeStamp(item.mtime).String())
                        }
                    } else {
                        break
                    }
                }
                for buffer.Len() > 0 {
                    if dryrun {
                        glog.Debugfln("%s writes: %d, array left: %d", path, buffer.Len(), array.Len())
                        buffer.Reset()
                        break
                    } else {
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
                }
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

// 自动归档检查循环，归档使用tar工具实现
func handlerArchiveCron() {
    if dryrun {
        return
    }
    paths, _ := gfile.ScanDir(logPath, "*.log", true)
    for _, path := range paths {
        // 日志文件超过30天不再更新，那么执行归档
        if gtime.Second() - gfile.MTime(path) < 30*86400 {
            continue
        }
        // 如果存在同名的压缩文件，那么采用文件名称++处理
        archivePath := path + ".tar.bz2"
        existIndex  := 1
        for gfile.Exists(archivePath) {
            archivePath = fmt.Sprintf("%s.%d.tar.bz2", path, existIndex)
            existIndex++
        }

        // 进入日志目录
        if err := os.Chdir(gfile.Dir(path)); err != nil {
            glog.Error(err)
            continue
        }
        // 执行日志文件归档，使用bzip2压缩格式
        cmd := fmt.Sprintf("tar -jvcf %s %s",  archivePath, gfile.Basename(path))
        glog.Debugfln(cmd)
        if err := gproc.ShellRun(cmd); err == nil {
            if err := gfile.Remove(path); err != nil {
                glog.Error(err)
            }
        } else {
            glog.Error(err)
        }
    }
}