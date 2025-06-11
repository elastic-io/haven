package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/elastic-io/haven/app"
	"github.com/elastic-io/haven/internal/clients"
	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/options"
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
			Name:  "kind, k",
			Usage: "kind of the artifact",
		},
		cli.StringFlag{
			Name:  "namespace, n",
			Usage: "namespace of the artifact",
		},
		cli.StringFlag{
			Name:  "path, p",
			Usage: "path to the directory to push",
		},
		cli.StringFlag{
			Name:  "limit, l",
			Value: "10G",
			Usage: "object size limit",
		},
		cli.StringSliceFlag{
			Name:  "exclude-exts",
			Value: &cli.StringSlice{".log", ".tmp", ".cache"},
			Usage: "exclude extensions",
		},
		cli.StringSliceFlag{
			Name:  "exclude-files",
			Value: &cli.StringSlice{},
			Usage: "exclude files",
		},
		cli.StringFlag{
			Name:  "ak",
			Value: "dummy-ak",
			Usage: "s3 ak",
		},
		cli.StringFlag{
			Name:  "sk",
			Value: "dummy-sk",
			Usage: "s3 sk",
		},
		cli.StringFlag{
			Name:  "token",
			Value: "dummy-token",
			Usage: "s3 token",
		},
		cli.StringFlag{
			Name:  "region",
			Value: "dummy-region",
			Usage: "s3 region",
		},
	},
	Action: func(ctx *cli.Context) error {
		if err := checkArgs(ctx, 1, exactArgs); err != nil {
			return fmt.Errorf("%s, please specify a id, format is [name:tag]", err)
		}
		return app.Main(ctx, NewPush, "HavenPushCommand")
	},
}

// excludeCallback 回调函数类型，返回 true 表示跳过该文件
type excludeCallback func(path string, info os.FileInfo) bool

type Push struct {
	config   *config.PushConfig
	s3Client clients.S3Client
}

func NewPush(opts *options.Options) (app.App, error) {
	push := &Push{config: config.NewPush(opts)}

	// TODO timeout
	var err error
	s3Endpoint := fmt.Sprintf("https://%s/s3", push.config.Endpoint)
	push.s3Client, err = clients.News3Client(push.config.AK, push.config.SK, push.config.Token, push.config.Region,
		s3Endpoint, clients.S3Options{
			S3ForcePathStyle:   true,
			DisableSSL:         true,
			InsecureSkipVerify: !push.config.Secure,
		})
	if err != nil {
		log.Logger.Error(err)
		return nil, err
	}

	return push, nil
}

func (p *Push) Run() error {
	menifest, err := p.getManifest()
	if err != nil {
		if err == clients.ErrS3KeyNotFound {
			return p.Exec()
		}
		return err
	}

	if err = p.checkpoint(menifest); err != nil {
		return err
	}

	return p.Exec()
}

func (p *Push) Exec() error {
	excludeCallback := func(path string, info os.FileInfo) bool {
		// 跳过隐藏文件
		if filepath.Base(path)[0] == '.' {
			return true
		}

		// 跳过大文件
		if info.Size() > int64(p.config.BigFileLimit) {
			return true
		}

		// 跳过特定文件名
		fileName := filepath.Base(path)
		for _, excludeFile := range p.config.ExcludeFiles {
			if fileName == excludeFile {
				return true
			}
		}

		// 跳过特定扩展名
		ext := filepath.Ext(path)
		for _, excludeExt := range p.config.ExcludeExts {
			if ext == excludeExt {
				return true
			}
		}

		return false
	}

	// 构建制品元信息(失败情况处理)
	manifest, manifestContent, mcBytes, err := p.buildManifest(p.config.Path, excludeCallback)
	if err != nil {
		return err
	}

	// 更新制品元信息到仓库
	manifestBytes, err := manifest.MarshalJSON()
	if err != nil {
		return err
	}
	// namespace=product_code, name=(app_code:tag)
	menifestKey := fmt.Sprintf("%s-%s", p.config.Namespace, fmt.Sprintf("%s:%s", p.config.Id))
	err = p.s3Client.UploadObject(p.config.Kind, menifestKey, manifestBytes)
	if err != nil {
		return err
	}

	// 先上传manifest config
	keys, err := p.s3Client.ListObjectsInBucket(p.config.Kind)
	if err != nil {
		return err
	}

	if !utils.FindKey(keys, manifest.Config.Digest) {
		err = p.s3Client.UploadObject(p.config.Kind, manifest.Config.Digest, mcBytes)
		if err != nil {
			return err
		}
	}

	// 根据制品元信息上传制品
	for _, layer := range manifest.Layers {
		// 跳过已经上传的文件
		if utils.FindKey(keys, layer.Digest) {
			log.Logger.Warnf("file %s already exists, skip", layer.Digest)
			continue
		}

		// 开始上传
		file := manifestContent.Files[layer.Digest]
		log.Logger.Infof("upload file: %s, dir: %s, kind: %s", file.Name, manifestContent.Dir, p.config.Kind)
		err = p.s3Client.UploadObjectStream(p.config.Kind, layer.Digest, filepath.Join(manifestContent.Dir, file.Name))
		if err != nil {
			log.Logger.Errorf("upload file failure, dir: %s, name: %s, err: %s", manifestContent.Dir, file.Name, err)
			continue
		}
		log.Logger.Infof("upload file: %s, dir: %s is ok", file.Name, manifestContent.Dir)

		// 结束验证

	}

	log.Logger.Info("Push Success")
	return nil
}

func (p *Push) Stop() error {
	return nil
}

func (p *Push) getManifest() (*types.ManifestV2, error) {
	var err error

	if err = p.s3Client.CreateBucket(p.config.Kind); err != nil {
		return nil, err
	}

	// ns=product_code, name=(app_code:tag)
	metaKey := fmt.Sprintf("%s-%s", p.config.Namespace, p.config.Id)
	artfact, err := p.s3Client.DownloadObject(p.config.Kind, metaKey)
	if err != nil {
		log.Logger.Error(err)
		return nil, err
	}

	m := &types.ManifestV2{}
	m.UnmarshalJSON(artfact)
	return m, nil
}

func (p *Push) getManifestContent(m *types.ManifestV2) (*types.ManifestV2Content, error) {
	b, err := p.s3Client.DownloadObject(p.config.Kind, m.Config.Digest)
	if err != nil {
		return nil, err
	}
	mc := &types.ManifestV2Content{}
	return mc, mc.UnmarshalJSON(b)
}

func (p *Push) checkpoint(m *types.ManifestV2) error {
	mc, err := p.getManifestContent(m)
	if err != nil {
		return err
	}
	// 检查新旧制品路径是否一致，如果不一致，则需要创建新的tag
	// 如果一致，则检查tag是否一致，如果不一致，则需要创建新的tag
	if p.config.Path != mc.Dir {
		if p.config.Id == mc.ID {
			return fmt.Errorf("dir not match, please create a new tag")
		}
	}

	// 如果id相同，则不进行制品上传
	if p.config.Id == mc.ID {
		log.Logger.Warnf("The name and tag have not changed. If you need to push the artiface, please change the tag.")
		return nil
	}
	return nil
}

func (p *Push) buildManifest(rootPath string, excludeCallback excludeCallback) (*types.ManifestV2, *types.ManifestV2Content, []byte, error) {
	m := &types.ManifestV2{SchemaVersion: int(time.Now().UnixMilli())}
	mc := &types.ManifestV2Content{Dir: rootPath}
	mc.Files = map[string]types.ManifestV2File{}

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过目录
		if info.IsDir() {
			return nil
		}

		// 调用回调函数检查是否跳过
		if excludeCallback != nil && excludeCallback(path, info) {
			log.Logger.Warnf("excludeping: %s", path)
			return nil
		}

		// 计算 SHA256
		SHA256Hash, err := utils.CalculateLargeFileSHA256(path)
		if err != nil {
			log.Logger.Errorf("Failed to calculate SHA256: %s, path: %s", err, path)
			return nil
		}

		// 构建 manifest
		m.Layers = append(m.Layers, types.ManifestV2Config{
			MediaType: p.config.Kind,
			Size:      int(info.Size()),
			Digest:    SHA256Hash,
		})

		// 构建 manifest content
		mc.Files[SHA256Hash] = types.ManifestV2File{
			ID:         utils.MustGetFileID(path),
			Name:       info.Name(),
			ModifyTime: time.Duration(info.ModTime().Unix()),
			Size:       info.Size(),
		}

		return nil
	})

	mcBytes, err := mc.MarshalJSON()
	if err != nil {
		return nil, nil, nil, err
	}

	// 构建 manifest config
	m.Config = types.ManifestV2Config{
		Size:   len(mcBytes),
		Digest: utils.ComputeSHA256(mcBytes),
	}

	return m, mc, mcBytes, err
}
