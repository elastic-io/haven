package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/elastic-io/haven/internal/clients"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/urfave/cli"
)

var pushCommand = cli.Command{
	Name:        "push",
	Usage:       "",
	ArgsUsage:   ``,
	Description: ``,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "path, p",
			Value: "",
			Usage: "path to the file to push",
		},
	},
	Action: func(ctx *cli.Context) error {
		artFile := ctx.String("path")
		dir := filepath.Dir(artFile)

		specFile := filepath.Join(dir, specConfig)
		if !utils.FileExist(specFile) {
			return fmt.Errorf(specConfig + " not found")
		}

		specBytes, err := os.ReadFile(specFile)
		if err != nil {
			return err
		}
		art := &types.Artifact{}
		art.UnmarshalJSON(specBytes)

		sec := ctx.GlobalBool("secure")
		endpoint := ctx.GlobalString("endpoint")
		ak := art.Annotations[types.AK]
		sk := art.Annotations[types.SK]
		token := art.Annotations[types.Token]
		region := art.Annotations[types.Region]

		s3Client, err := clients.News3Client(ak, sk, token, region, endpoint, clients.S3Options{
			S3ForcePathStyle:   true,
			DisableSSL:         true,
			InsecureSkipVerify: !sec,
		})
		if err != nil {
			log.Logger.Error(err)
			return err
		}

		// ns=product_code, name=app_code
		return s3Client.UploadObject(art.Kind, fmt.Sprintf("%s/%s/%s", art.Namespace, art.Name, art.UID), []byte(artFile))
	},
}
