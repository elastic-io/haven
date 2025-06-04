package main

import "github.com/elastic-io/haven/cmd/hvn/cmd"

// version must be set from the contents of VERSION file by go build's
// -X main.version= option in the Makefile.
var version = "unknown"

// gitCommit will be the hash that the binary was built from
// and will be populated by the Makefile
var gitCommit = ""

const (
	usage = `
    # hvn
`
)

func main() {
	cmd.Execute("hvn", usage, version, gitCommit)
}

/*
import (
	"fmt"

	"github.com/elastic-io/haven/internal/clients"
	"github.com/elastic-io/haven/internal/log"
)

const (
	region   = "us-east-1"
	endpoint = "https://10.253.61.103:8900/s3"
	ak       = "havenadmin"
	sk       = "havenadmin"
	token    = ""
	bucket   = "mybucket"
)

func main() {
	log.Init("", "debug")
	s3Client, err := clients.News3Client(ak, sk, token, region, endpoint, clients.S3Options{
		S3ForcePathStyle:   true,
		DisableSSL:         true,
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Logger.Panic(err)
	}

	err = s3Client.CreateBucket(bucket)
	if err != nil {
		log.Logger.Panic(err)
	}
	log.Logger.Info("bucket created")

	buckets, err := s3Client.ListBuckets()
	if err != nil {
		log.Logger.Panic(err)
	}

	for _, b := range buckets {
		fmt.Println(b)
	}

	err = s3Client.DeleteBucket(bucket)
	if err != nil {
		log.Logger.Panic(err)
	}
	log.Logger.Info("bucket deleted")

	buckets, err = s3Client.ListBuckets()
	if err != nil {
		log.Logger.Panic(err)
	}

	for _, b := range buckets {
		fmt.Println(b)
	}
}
*/
