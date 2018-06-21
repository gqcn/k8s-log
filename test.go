package main

import (
    "gitee.com/johng/gf/g/os/gtime"
    "fmt"
    "time"
)

func main() {
    fmt.Println(time.Time{}.IsZero())
    if t, err := gtime.StrToTime("2018-02-02 01:00:00"); err == nil {
        t = t.Add(8 * time.Hour)
        fmt.Println(t.IsZero())
    }
}
