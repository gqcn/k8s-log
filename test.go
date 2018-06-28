package main

import (
    "fmt"
    "strings"
    "gitee.com/johng/gf/g/os/gfile"
)

func main() {
    a := strings.Split(gfile.Dir("/var/log/medlinker/test-app/test.log"), "/")
    for k, v := range a {
        fmt.Println(k, v)
    }
}
