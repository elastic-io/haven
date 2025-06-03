package main

import "github.com/elastic-io/haven/cmd"

// version must be set from the contents of VERSION file by go build's
// -X main.version= option in the Makefile.
var version = "unknown"

// gitCommit will be the hash that the binary was built from
// and will be populated by the Makefile
var gitCommit = ""

const (
	usage = `
To start a new instance of a artifactory repository:
    # haven run -e 127.0.0.1:8080
`
)

func main() {
	cmd.Execute("haven", usage, version, gitCommit)
}
