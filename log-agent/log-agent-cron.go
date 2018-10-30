package main

import (
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
)

// 自动清理日志文件
func cleanLogCron() {
    if list, err := gfile.ScanDir(logPath, "*", true); err == nil {
        for _, path := range list {
            if !gfile.IsFile(path) || gfile.Size(path) < cleanMinSize {
                continue
            }
            if gtime.Second() - gfile.MTime(path) > bufferTime {
                // 一定内没有任何更新操作，则truncate
                glog.Debug("truncate expired file:", path)
                if err := gfile.Truncate(path, 0); err == nil {
                    offsetMapCache.Remove(path)
                    offsetMapSave.Remove(path)
                } else {
                    glog.Error(err)
                }
            } else {
                // 判断文件大小，超过指定大小则truncate
                if gfile.Size(path) > cleanMaxSize {
                    glog.Debug("truncate size-exceeded file:", path)
                    if err := gfile.Truncate(path, 0); err == nil {
                        offsetMapCache.Remove(path)
                        offsetMapSave.Remove(path)
                    } else {
                        glog.Error(err)
                    }
                } else {
                    glog.Debug("leave alone file:", path)
                }
            }
        }
    } else {
        glog.Error(err)
    }
}

// 定时保存日志文件的offset记录到文件中
func saveOffsetCron() {
    if offsetMapSave.Size() == 0 {
        return
    }
    if content, err := gjson.Encode(offsetMapSave.Clone()); err != nil {
        glog.Error(err)
    } else {
        if err := gfile.PutBinContents(offsetFilePath, content); err != nil {
            glog.Error(err)
        }
    }
}
