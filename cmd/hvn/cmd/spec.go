package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/elastic-io/haven/internal/types"
	"github.com/urfave/cli"
)

const specConfig = "spec.json"

var specCommand = cli.Command{
	Name:        "spec",
	Usage:       "generate spec file",
	ArgsUsage:   ``,
	Description: `generate spec file`,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "kind, k",
			Value: "",
			Usage: "kind of the artifact",
		},
		cli.StringFlag{
			Name:  "name, n",
			Value: "",
			Usage: "name of the artifact",
		},
		cli.StringFlag{
			Name:  "namespace, ns",
			Value: "",
			Usage: "namespace of the artifact",
		},
	},
	Action: func(ctx *cli.Context) error {
		kind := ctx.String("kind")
		if kind == "" {
			return fmt.Errorf(
				"kind is required. use --kind to specify it")
		}
		name := ctx.String("name")
		if name == "" {
			return fmt.Errorf(
				"name is required. use --name to specify it")
		}
		namespace := ctx.String("namespace")
		if namespace == "" {
			return fmt.Errorf(
				"namespace is required. use --namespace to specify it")
		}
		spec := artifact(kind, name, namespace)
		checkNoFile := func(name string) error {
			_, err := os.Stat(name)
			if err == nil {
				return fmt.Errorf("file %s exists. remove it first", name)
			}
			if !os.IsNotExist(err) {
				return err
			}
			return nil
		}
		if err := checkNoFile(specConfig); err != nil {
			return err
		}
		data, err := spec.MarshalJSON()
		if err != nil {
			return err
		}
		return os.WriteFile(specConfig, data, 0o666)
	},
}

func artifact(kind, name, ns string) *types.Artifact {
	return &types.Artifact{
		TypeMeta: types.TypeMeta{
			APIVersion: "haven.elastic.io/v1",
			Kind:       kind,
		},
		ObjectMeta: types.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			CreationTimestamp: time.Now(),
			Annotations: map[string]string{
				types.AK:     "dummy-ak",
				types.SK:     "dummy-sk",
				types.Token:  "dummy-token",
				types.Region: "dummy-region",
			},
		},
	}
}
