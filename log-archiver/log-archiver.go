// 定时将30天之前/或者大小超过指定限制的数据进行压缩归档并删除(原始日志文件保留30天)，时间可通过环境变量配置。

package main

import (
    "fmt"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gproc"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "os"
)

const (
    LOG_PATH  = "/var/log/medlinker" // 日志目录
    EXPIRE    = "30"                 // 过期时间(天)
    MAX_BYTES = "10240"              // 单文件最大大小限制(MB)
    DEBUG     = "true"                // 默认值，是否打开调试信息
)

var (
    logPath  = genv.Get("LOG_PATH", LOG_PATH)
    expire   = gconv.Int64(genv.Get("EXPIRE", EXPIRE))
    maxBytes = gconv.Int64(genv.Get("MAX_BYTES", MAX_BYTES))*1024*1024
    debug    = gconv.Bool(genv.Get("DEBUG", DEBUG))
)

func main() {
    // 是否显示调试信息
    glog.SetDebug(debug)

    // 定时压缩归档任务，凌晨执行
    gcron.Add("0 0 2 * * *", handlerArchiveCron)

    // 阻塞运行
    select { }
}

// 自动归档检查循环，归档使用tar工具实现
func handlerArchiveCron() {
    paths, _ := gfile.ScanDir(logPath, "*", true)
    for _, path := range paths {
        // 不处理目录、kafka offset文件、已经压缩过的文件
        if gfile.IsDir(path) || gfile.Ext(path) == ".offset" || gfile.Ext(path) == ".bz2" {
            glog.Debugfln(`ignore file type %s`, path)
            continue
        }
        size := gfile.Size(path)
        if size < maxBytes {
            // 日志文件超过30天不再更新，那么执行归档
            if gtime.Second() - gfile.MTime(path) < expire*86400 {
                glog.Debugfln(`file not expired %s`, path)
                continue
            }
        } else {
            // 超过文件大小限制(rename处理，dumper会识别到命名改变，更新文件指针)
            renamePath := path + ".1"
            existIndex := 2
            for gfile.Exists(renamePath) {
                renamePath = fmt.Sprintf("%s.%d", path, existIndex)
                existIndex++
            }
            gfile.Rename(path, renamePath)
            path = renamePath
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
            glog.Error(path, err)
            continue
        }
        // 执行日志文件归档，使用bzip2压缩格式
        cmd := fmt.Sprintf("tar -jvcf %s %s",  archivePath, gfile.Basename(path))
        glog.Debug(cmd)
        if err := gproc.ShellRun(cmd); err == nil {
            if err := gfile.Remove(path); err != nil {
                glog.Error(path, err)
            }
        } else {
            glog.Error(path, err)
        }
    }
}