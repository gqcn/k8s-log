package main

import (
    "bytes"
    "fmt"
    "gitee.com/johng/gf/g/container/garray"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gproc"
    "gitee.com/johng/gf/g/os/gtime"
    "os"
    "time"
)

// 异步批量保存日志
func handlerSavingContent() {
    // 批量写日志
    for _, path := range bufferMap.Keys() {
        array  := bufferMap.Get(path).(*garray.SortedArray)
        if array.Len() > 0 {
            //glog.Debugfln("%s array: %d", path, array.Len())
            buffer := bytes.NewBuffer(nil)
            mtime  := gtime.Millisecond() - bufferTime*1000
            for i := 0; i < array.Len(); i++ {
                item := array.Get(0).(*bufferItem)
                // 超过缓冲区时间则写入文件
                if item.mtime <= mtime {
                    //glog.Debugfln(`%s : item.mtime(%d) <= mtime(%d)`, path, item.mtime, mtime)
                    buffer.WriteString(item.content)
                    array.Remove(0)
                    if dryrun {
                        //glog.Debugfln(`%s item: %s`, path, gtime.NewFromTimeStamp(item.mtime).String())
                    }
                } else {
                    //glog.Debugfln(`%s : item.mtime(%d) > mtime(%d)`, path, item.mtime, mtime)
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
                        glog.Debugfln("%s writes: %d bytes, array left: %d items", path, buffer.Len(), array.Len())
                        buffer.Reset()
                        break
                    }
                }

            }
        }
    }

    // 导出topic offset到磁盘保存
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