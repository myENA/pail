# pail
Lightweight extension to couchbase/gocb bucket that provides CRUD and N1QL retry logic

[![](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://godoc.org/github.com/myENA/pail)

## Purpose
In our testing with Couchbase, we noticed we sometimes see errors that stem from either an internal lock within the
gocb client, sporadic networking issues, etc etc.  These errors have nothing to do with the request being executed and
there is nothing to "handle" within our app code, thus the only solution was to catch every error and just try it again.

This package is the end result of that work.  Its only dependency is the upstream 
[gocb](https://github.com/couchbase/gocb) package, and provides glide, dep, and mod dependency manager files.

## Basic Usage

```go
package main

import(
	"fmt"
	
    "github.com/couchbase/gocb/v2"
    "github.com/myENA/pail/v2"
)

func main() {
	// create couchbase connection and bucket as you normally would
	connStr := "couchbase://127.0.0.1"
    cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{})
    if err != nil {
    	panic(err)
    }
    cluster.
    bucket, err := cluster.OpenBucket("mybuck", "mypass")
    if err != nil {
    	panic(err)
    }
    
    // once created, make a new pail 
    p, err := pail.New(bucket, 5)
    if err != nil {
    	panic(err)
    }
    
    // From here, the API is pretty simple.  Any call you wish to attempt retries on, execute the "TryX" version of the
    // standard api method
    
    type pType struct {
    	Key string
    	Value string
    }
    
    tPtr := new(pType)
    
    // TryGet wraps bucket.Get
    cas, err := p.TryGet("mykey", tPtr)
    if err != nil {
    	panic(err)
    }
    fmt.Println(cas)
}

```