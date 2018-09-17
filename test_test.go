package main

import (
    "testing"
    "gitee.com/johng/gf/g/os/gfile"
    "os"
    "gitee.com/johng/gf/g/os/gfpool"
)

func Benchmark_os_Open_Close(b *testing.B) {
    for i := 0; i < b.N; i++ {
        f, _ := os.OpenFile("/tmp/bench-test", os.O_RDWR|os.O_APPEND, 0766)
        f.Close()
    }
}

func Benchmark_gfpool_Open_Close(b *testing.B) {
    for i := 0; i < b.N; i++ {
        f, _ := gfpool.Open("/tmp/bench-test", os.O_RDWR|os.O_APPEND, 0766)
        f.Close()
    }
}

func Benchmark_gfile_PutContentsAppend(b *testing.B) {
    for i := 0; i < b.N; i++ {
        gfile.PutContentsAppend("/tmp/bench-test", "1")
    }
}
