// 容器日志清理客户端.

package main

import (
    "gitee.com/johng/gf/g/os/gcmd"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gproc"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
)

const (
    CLEAN_LOG_PATH    = "/var/log/medlinker"         // 默认值，日志清理目录绝对路径
    CLEAN_CRONTAB     = "0 0 * * * *"                // 默认值，清理定时任务设置(默认每天凌晨3点执行)
    CLEAN_BUFFER_TIME = "3600"                       // 默认值，(秒)当执行清理时，超过多少时间没有更新则执行删除(默认1小时)
    CLEAN_MAX_SIZE    = "1073741824"                 // 默认值，(byte)日志文件最大限制，当清理时执行规则处理(默认1GB)
    CLEAN_DEBUG       = "true"                       // 默认值，是否打开调试信息
)

var (
    logPath      = genv.Get("CLEAN_LOG_PATH", CLEAN_LOG_PATH)
    crontab      = genv.Get("CLEAN_CRONTAB", CLEAN_CRONTAB)
    bufferTime   = gconv.Int64(genv.Get("CLEAN_BUFFER_TIME", CLEAN_BUFFER_TIME))
    cleanMaxSize = gconv.Int64(genv.Get("CLEAN_MAX_SIZE", CLEAN_MAX_SIZE))
    dryrun       = gconv.Bool(gcmd.Option.Get("dryrun", "0"))
    debug        = gconv.Bool(genv.Get("CLEAN_DEBUG", CLEAN_DEBUG))
)

func main() {
    glog.Printfln("%d: log-agent starts running.", gproc.Pid())
    glog.SetDebug(debug)

    // 每个小时执行清理工作
    if !dryrun {
        gcron.Add(crontab, cleanLogCron)
        select{}
    } else {
        cleanLogCron()
    }
}

// 自动清理日志文件
func cleanLogCron() {
    if list, err := gfile.ScanDir(logPath, "*.log", true); err == nil {
        for _, path := range list {
            if !gfile.IsFile(path) || gfile.Size(path) == 0 {
                continue
            }
            if gtime.Second() - gfile.MTime(path) > bufferTime {
                // 一定内没有任何更新操作，如果文件名称带日期则删除，否则truncate
                err := (error)(nil)
                if gtime.ParseTimeFromContent(path, "Ymd") != nil ||
                    gtime.ParseTimeFromContent(path, "Y-m-d") != nil {
                    glog.Debug("[log-clean] remove expired file:", path)
                    if !dryrun {
                        err = gfile.Remove(path)
                    }
                } else {
                    glog.Debug("[log-clean] truncate expired file:", path)
                    if !dryrun {
                        err = gfile.Truncate(path, 0)
                    }
                }
                if err != nil {
                    glog.Error(err)
                }
            } else {
                // 判断文件大小，超过指定大小则truncate
                if gfile.Size(path) > cleanMaxSize {
                    glog.Debug("[log-clean] truncate size-exceeded file:", path)
                    if !dryrun {
                        if err := gfile.Truncate(path, 0); err != nil {
                            glog.Error(err)
                        }
                    }
                } else {
                    glog.Debug("[log-clean] leave alone file:", path)
                }
            }
        }
    } else {
        glog.Error(err)
    }
}