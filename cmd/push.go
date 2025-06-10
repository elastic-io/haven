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
			Name:  "path, p",
			Value: "",
			Usage: "path to the file to push",
		},
		cli.StringFlag{
			Name:  "tag, t",
			Value: "",
			Usage: "tag to push",
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
	},
	Action: func(ctx *cli.Context) error {
		if err := checkArgs(ctx, 1, exactArgs); err != nil {
			return fmt.Errorf("%s, please specify a tag", err)
		}
		return app.Main(ctx, NewPush, "HavenPushCommand")
	},
}

// excludeCallback 回调函数类型，返回 true 表示跳过该文件
type excludeCallback func(path string, info os.FileInfo) bool

type Push struct {
	config   *config.PushConfig
	s3Client clients.S3Client
	art      *types.Artifact
}

func NewPush(opts *options.Options) (app.App, error) {
	push := &Push{config: config.NewPush(opts), art: &types.Artifact{}}

	dir := filepath.Dir(push.config.Path)
	specFile := filepath.Join(dir, specConfig)
	if !utils.FileExist(specFile) {
		return nil, fmt.Errorf(specConfig + " not found")
	}

	specBytes, err := os.ReadFile(specFile)
	if err != nil {
		return nil, err
	}

	push.art.UnmarshalJSON(specBytes)

	push.art.Spec.Dir = dir

	ak := push.art.Annotations[types.AK]
	sk := push.art.Annotations[types.SK]
	token := push.art.Annotations[types.Token]
	region := push.art.Annotations[types.Region]

	// TODO timeout
	s3Endpoint := fmt.Sprintf("https://%s/s3", push.config.Endpoint)
	push.s3Client, err = clients.News3Client(ak, sk, token, region, s3Endpoint, clients.S3Options{
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
	previousArt, err := p.getArtfact()
	if err != nil {
		if err == clients.ErrS3KeyNotFound {
			return p.Exec()
		}
		return err
	}

	if err = p.checkpoint(previousArt); err != nil {
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
		excludeFiles := []string{specConfig}
		if files := p.config.ExcludeFiles; files != nil {
			excludeFiles = append(excludeFiles, files...)
		}
		for _, excludeFile := range excludeFiles {
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
	art, err := p.buildArtfact(p.art.Spec.Dir, excludeCallback)
	if err != nil {
		return err
	}

	// 更新制品元信息到仓库
	artBytes, err := art.MarshalJSON()
	if err != nil {
		return err
	}
	// ns=product_code, name=(app_code:tag)
	artKey := fmt.Sprintf("%s-%s", p.art.Namespace, fmt.Sprintf("%s:%s", p.art.Name, p.config.Tag))
	err = p.s3Client.UploadObject(p.art.Kind, artKey, artBytes)
	if err != nil {
		return err
	}

	// 根据制品元信息上传制品
	for _, file := range art.Spec.Files {
		log.Logger.Infof("upload file: %s, dir: %s, kind: %s", file.Name, p.art.Spec.Dir, p.art.Kind)
		err := p.s3Client.UploadObjectStream(p.art.Kind, file.MD5,
			filepath.Join(p.art.Spec.Dir, file.Name))
		if err != nil {
			log.Logger.Errorf("upload file failure, dir: %s, name: %s, err: %s", p.art.Spec.Dir, file.Name, err)
			continue
		}
		log.Logger.Infof("upload file: %s, dir: %s is ok", file.Name, p.art.Spec.Dir)
	}

	// first push, caculateMD5, status add tag
	// 检查artifact里的status的tag字段，如果前序tag与当前的一致，则不更新
	// 如果不一致，则更新，计算指定文件夹里的所有文件的md5, 一致则不推送，不一致则推送，组织新的tag

	log.Logger.Info("Push Success")
	return nil
}

func (p *Push) Stop() error {
	return nil
}

func (p *Push) getArtfact() (*types.Artifact, error) {
	var err error

	if err = p.s3Client.CreateBucket(p.art.Kind); err != nil {
		return nil, err
	}

	// ns=product_code, name=(app_code:tag)
	metaKey := fmt.Sprintf("%s-%s", p.art.Namespace, fmt.Sprintf("%s:%s", p.art.Name, p.config.Tag))
	artfact, err := p.s3Client.DownloadObject(p.art.Kind, metaKey)
	if err != nil {
		log.Logger.Error(err)
		return nil, err
	}

	art := &types.Artifact{}
	art.UnmarshalJSON(artfact)
	return art, nil
}

func (p *Push) checkpoint(old *types.Artifact) error {
	// 检查新旧制品路径是否一致，如果不一致，则需要创建新的tag
	// 如果一致，则检查tag是否一致，如果不一致，则需要创建新的tag
	if p.art.Spec.Dir != old.Spec.Dir {
		if p.art.Status.Tag.Current == old.Status.Tag.Current {
			return fmt.Errorf("dir not match, please create a new tag")
		}
	}
	return nil
}

func (p *Push) buildArtfact(rootPath string, excludeCallback excludeCallback) (*types.Artifact, error) {
	var fis []types.FileInfo

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

		// 计算 MD5
		md5Hash, md5Err := utils.CalculateLargeFileMD5(path)
		fi := types.FileInfo{
			Name:       info.Name(),
			Size:       info.Size(),
			ModifyTime: time.Duration(info.ModTime().Unix()),
			MD5:        md5Hash,
			Err:        md5Err,
		}

		// 获取系统文件ID
		fid, err := utils.GetFileID(path)
		if err != nil {
			log.Logger.Errorf("Failed to get file ID: %s, path: %s", err, path)
		} else {
			fi.ID = fid
		}

		fis = append(fis, fi)

		return nil
	})

	p.art.Spec.Files = fis
	return p.art, err
}