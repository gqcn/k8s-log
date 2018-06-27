package main

import (
    "fmt"
    "strings"
    "gitee.com/johng/gf/g/os/gfile"
)

func main() {
    fmt.Println(strings.Split(gfile.Dir("/var/log/medlinker/test-app/test.log"), "/"))
}
