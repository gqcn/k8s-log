package main

import (
    "fmt"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "os"
    "os/exec"
)

// 自动归档检查循环，归档使用tar工具实现
func handlerArchiveCron() {
    paths, _ := gfile.ScanDir(logPath, "*.log", true)
    for _, path := range paths {
        // 日志文件超过30天，那么执行归档
        if gtime.Second() - gfile.MTime(path) < 30*86400 {
            continue
        }
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
        // 执行日志文件归档
        cmd := exec.Command("tar", "-jvcf",  archivePath, gfile.Basename(path))
        glog.Debugfln("tar -jvcf %s %s", archivePath, gfile.Basename(path))
        if err := cmd.Run(); err == nil {
            if err := gfile.Remove(path); err != nil {
                glog.Error(err)
            }
        } else {
            glog.Error(err)
        }
    }
}