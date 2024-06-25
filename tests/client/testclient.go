package main

import (
	"context"
	"fmt"
	"github.com/gkalele/dfs/gcsfs"
	"os"
)

func main() {
	bucketName := os.Args[1]
	fmt.Printf("Testing GCSFS client using bucket %s\n", bucketName)
	fs := gcsfs.New(bucketName, gcsfs.Panic)

	ctx := context.Background()
	stat, err := fs.StatFs(ctx)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("StatFs GetName(): %s\n", stat.GetName())

	if err = fs.CreateEmptyFile(ctx, "MARKER"); err != nil {
		panic(err.Error())
	}
	fmt.Printf("CreateEmptyFile successful\n")
	if err = fs.Rename(ctx, "MARKER", "NEWMARKER"); err != nil {
		panic(err.Error())
	}
	fmt.Printf("Renamed MARKER to NEWMARKER\n")
}
