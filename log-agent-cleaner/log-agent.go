// 容器日志清理客户端.

package main

import (
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
)

const (
    LOG_PATH = "/var/log/medlinker"         // 默认值，日志清理目录绝对路径
    CRONTAB  = "0 0 3 * * *"                // 默认值，清理定时设置
    DEBUG    = "true"                       // 默认值，是否打开调试信息
)

var (
    logPath  = genv.Get("LOG_PATH", LOG_PATH)
    crontab  = genv.Get("CRONTAB", CRONTAB)
    debug    = gconv.Bool(genv.Get("DEBUG", DEBUG))
)

func main() {
    glog.SetDebug(debug)

    // 每个小时执行清理工作
    gcron.Add(crontab, cleanLogCron)

    select{}
}

// 自动清理日志文件
func cleanLogCron() {
    if list, err := gfile.ScanDir(logPath, "*", true); err == nil {
        for _, path := range list {
            if !gfile.IsFile(path) {
                continue
            }
            // 如果文件名称带日期则删除，否则truncate
            err := (error)(nil)
            if gtime.ParseTimeFromContent(path, "Ymd") != nil ||
                gtime.ParseTimeFromContent(path, "Y-m-d") != nil {
                err = gfile.Remove(path)
                glog.Debug("remove file:", path)
            } else {
                err = gfile.Truncate(path, 0)
                glog.Debug("truncate file:", path)
            }
            if err != nil {
                glog.Error(err)
            }
        }
    } else {
        glog.Error(err)
    }
}