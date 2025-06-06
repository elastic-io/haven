package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/elastic-io/haven/internal/service"
	"github.com/elastic-io/haven/internal/types"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/gofiber/fiber/v2"

	"github.com/elastic-io/haven/internal/api"
	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
)

func init() {
	API := RegistryAPI{name: "registry"}
	api.APIRegister(API.name, &API)
}

type RegistryAPI struct {
	name            string
	streamThreshold int
	service         service.RegistryService
}

func (api *RegistryAPI) Init(c *config.Config) {
	service, err := service.NewRegistryService(c.Storage)
	if err != nil {
		panic(fmt.Errorf(err.Error()))
	}
	if err = service.Init(c); err != nil {
		panic(fmt.Errorf(err.Error()))
	}
	api.streamThreshold = 64 * types.MB
	api.service = service
}

func (api *RegistryAPI) RegisterRoutes(app *fiber.App) {
	log.Logger.Info("Registering Docker Registry API routes")

	// API v2 基础路由
	app.Get("/v2/", api.handleAPIBase)
	app.Head("/v2/", api.handleAPIBase)

	// 使用中间件处理所有 v2 路径
	app.Use("/v2/", api.registryPathMiddleware)

	// 使用通配符路由捕获所有可能的路径
	app.Post("/v2/*", func(c *fiber.Ctx) error {
		path := c.Path()
		if strings.HasSuffix(path, "/blobs/uploads/") {
			return api.handleStartUpload(c)
		}
		return c.Next()
	})

	// 其他路由保持不变
	app.Get("/v2/*", func(c *fiber.Ctx) error {
		path := c.Path()
		if strings.Contains(path, "/manifests/") {
			return api.handlePullManifest(c)
		} else if strings.Contains(path, "/blobs/") && !strings.Contains(path, "/uploads/") {
			return api.handlePullBlob(c)
		}
		return c.Next()
	})

	app.Head("/v2/*", func(c *fiber.Ctx) error {
		path := c.Path()
		if strings.Contains(path, "/manifests/") {
			return api.handlePullManifest(c)
		} else if strings.Contains(path, "/blobs/") && !strings.Contains(path, "/uploads/") {
			return api.handlePullBlob(c)
		}
		return c.Next()
	})

	app.Put("/v2/*", func(c *fiber.Ctx) error {
		path := c.Path()
		if strings.Contains(path, "/manifests/") {
			return api.handlePushManifest(c)
		} else if strings.Contains(path, "/blobs/uploads/") {
			return api.handlePushBlob(c)
		}
		return c.Next()
	})

	app.Patch("/v2/*", func(c *fiber.Ctx) error {
		if strings.Contains(c.Path(), "/blobs/uploads/") {
			return api.handleBlobUploadPatch(c)
		}
		return c.Next()
	})

	app.Delete("/v2/*", func(c *fiber.Ctx) error {
		path := c.Path()
		if strings.Contains(path, "/manifests/") {
			return api.handleDeleteManifest(c)
		} else if strings.Contains(path, "/blobs/") && !strings.Contains(path, "/uploads/") {
			return api.handleDeleteBlob(c)
		}
		return c.Next()
	})

	// 垃圾回收路由
	app.Post("/v2/_gc", api.handleGarbageCollection)

	app.Get("/v2/_debug/manifests", api.handleListManifests)
}

// registryPathMiddleware 函数
func (api *RegistryAPI) registryPathMiddleware(c *fiber.Ctx) error {
	path := c.Path()

	// 处理 blob 上传初始化路由 (POST /v2/{repository}/blobs/uploads/)
	if c.Method() == "POST" && strings.HasSuffix(path, "/blobs/uploads/") {
		repository := strings.TrimPrefix(path, "/v2/")
		repository = strings.TrimSuffix(repository, "/blobs/uploads/")
		c.Locals("repository", repository)
		return c.Next()
	}

	// 其他路由处理保持不变...
	// 处理清单路由
	if strings.Contains(path, "/manifests/") {
		parts := strings.Split(path, "/manifests/")
		if len(parts) == 2 {
			repository := strings.TrimPrefix(parts[0], "/v2/")
			reference := parts[1]
			c.Locals("repository", repository)
			c.Locals("reference", reference)
		}
	}

	// 处理blob获取/删除路由
	if strings.Contains(path, "/blobs/") && !strings.Contains(path, "/uploads/") {
		parts := strings.Split(path, "/blobs/")
		if len(parts) == 2 {
			repository := strings.TrimPrefix(parts[0], "/v2/")
			digest := parts[1]
			c.Locals("repository", repository)
			c.Locals("digest", digest)
		}
	}

	// 处理blob上传路由
	if strings.Contains(path, "/blobs/uploads/") && !strings.HasSuffix(path, "/uploads/") {
		parts := strings.Split(path, "/blobs/uploads/")
		if len(parts) == 2 {
			repository := strings.TrimPrefix(parts[0], "/v2/")
			uuid := parts[1]
			c.Locals("repository", repository)
			c.Locals("uuid", uuid)
		}
	}

	return c.Next()
}

func (r *RegistryAPI) handleStartUpload(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称
	repository := c.Locals("repository").(string)

	log.Logger.Info("Start upload for repository: ", repository)

	uploadID := utils.GenerateUploadUUID()

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, uploadID)
	c.Set("Location", location)
	c.Set("Docker-Upload-UUID", uploadID)
	return c.SendStatus(fiber.StatusAccepted)
}

// API 基础路由处理
func (r *RegistryAPI) handleAPIBase(c *fiber.Ctx) error {
	c.Set("Docker-Distribution-API-Version", "registry/2.0")
	return c.SendStatus(fiber.StatusOK)
}

// 处理镜像清单推送
func (r *RegistryAPI) handlePushManifest(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和引用
	repository := c.Locals("repository").(string)
	reference := c.Locals("reference").(string)

	log.Logger.Info("Push manifest: ", repository, ":", reference)

	contentType := c.Get("Content-Type")
	body := c.Body()

	log.Logger.Info("Received manifest: ", len(body), " bytes, ", "content-type: ", contentType)

	digest := "sha256:" + utils.ComputeSHA256(body)
	log.Logger.Info("Computed digest: ", digest)

	if err := r.service.PutManifest(repository, reference, body, contentType); err != nil {
		log.Logger.Error("Failed to store manifest: ", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to store manifest")
	}

	log.Logger.Info("Successfully stored manifest for ", repository, ":", reference)

	c.Set("Docker-Content-Digest", digest)
	return c.SendStatus(fiber.StatusCreated)
}

// 处理Blob分块上传 (PATCH 请求)
/*
func (r *RegistryAPI) handleBlobUploadPatch(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和UUID
	repository := c.Locals("repository").(string)
	uuid := c.Locals("uuid").(string)

	log.Logger.Info("Patch blob: ", repository, " uuid: ", uuid)

	// 获取上传数据对象
	uploadData := getOrCreateUploadData(repository, uuid)

	// 检查是否为流式请求
	if c.Request().IsBodyStream() {
		log.Logger.Info("Processing streaming request for blob patch")
		// 使用流式处理
		if err := uploadData.AppendStream(c.Request().BodyStream()); err != nil {
			log.Logger.Error("Failed to append stream data: ", err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
	} else {
		// 追加数据块
		if err := uploadData.AppendData(c.Body()); err != nil {
			log.Logger.Error("Failed to append data: ", err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
	}

	// 设置响应头
	c.Set("Range", fmt.Sprintf("0-%d", uploadData.Size()-1))
	c.Set("Docker-Upload-UUID", uuid)
	c.Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, uuid))
	return c.SendStatus(fiber.StatusAccepted)
}
*/

// 在 handleBlobUploadPatch 方法中添加错误处理
func (r *RegistryAPI) handleBlobUploadPatch(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和UUID
	repository := c.Locals("repository").(string)
	uuid := c.Locals("uuid").(string)

	log.Logger.Info("Patch blob: ", repository, " uuid: ", uuid)

	// 获取上传数据对象
	uploadData := getOrCreateUploadData(repository, uuid)

	// 检查是否为流式请求
	if c.Request().IsBodyStream() {
		log.Logger.Info("Processing streaming request for blob patch")

		// 使用安全的方式处理流
		bodyStream := c.Request().BodyStream()
		if bodyStream == nil {
			log.Logger.Error("Nil body stream received")
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		// 包装读取器以捕获错误
		safeReader := &safeReader{r: bodyStream}

		// 使用流式处理
		if err := uploadData.AppendStream(safeReader); err != nil {
			log.Logger.Error("Failed to append stream data: ", err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
	} else {
		// 追加数据块
		if err := uploadData.AppendData(c.Body()); err != nil {
			log.Logger.Error("Failed to append data: ", err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
	}

	// 设置响应头
	c.Set("Range", fmt.Sprintf("0-%d", uploadData.Size()-1))
	c.Set("Docker-Upload-UUID", uuid)
	c.Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, uuid))
	return c.SendStatus(fiber.StatusAccepted)
}

// 添加一个安全的读取器包装器
type safeReader struct {
	r io.Reader
}

func (sr *safeReader) Read(p []byte) (n int, err error) {
	if sr.r == nil {
		return 0, io.EOF
	}

	defer func() {
		if r := recover(); r != nil {
			log.Logger.Error("Recovered from panic in reader: ", r)
			err = fmt.Errorf("reader panic: %v", r)
			n = 0
		}
	}()

	return sr.r.Read(p)
}

// 处理Blob推送
func (r *RegistryAPI) handlePushBlob(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和UUID
	repository := c.Locals("repository").(string)
	uuid := c.Locals("uuid").(string)

	digest := c.Query("digest")
	if digest == "" {
		return c.Status(fiber.StatusBadRequest).SendString("Missing digest parameter")
	}

	log.Logger.Info("Push blob: ", repository, " uuid: ", uuid, " digest: ", digest)

	// 创建临时文件用于处理和验证数据
	tempFile, err := os.CreateTemp("", "blob-*")
	if err != nil {
		log.Logger.Error("Failed to create temp file: ", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to create temp file")
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	defer tempFile.Close()

	// 创建哈希计算器
	hasher := sha256.New()
	multiWriter := io.MultiWriter(tempFile, hasher)

	// 处理请求体或上传数据
	if c.Request().Header.ContentLength() > 0 {
		// 有请求体
		if c.Request().IsBodyStream() {
			// 流式处理
			log.Logger.Info("Processing streaming request body")
			if _, err := io.Copy(multiWriter, c.Request().BodyStream()); err != nil {
				log.Logger.Error("Failed to process request stream: ", err)
				return c.Status(fiber.StatusInternalServerError).SendString("Failed to process request body")
			}
		} else {
			// 非流式处理，但直接写入文件而不是加载到内存
			if _, err := multiWriter.Write(c.Body()); err != nil {
				log.Logger.Error("Failed to write request body: ", err)
				return c.Status(fiber.StatusInternalServerError).SendString("Failed to process request body")
			}
		}
	} else {
		// 从临时存储获取之前通过PATCH上传的数据
		uploadData := getUploadData(repository, uuid)
		if uploadData == nil {
			return c.Status(fiber.StatusNotFound).SendString("No upload data found")
		}

		// 流式写入临时文件和哈希计算器
		if err := uploadData.WriteTo(multiWriter); err != nil {
			log.Logger.Error("Failed to process upload data: ", err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}

		// 清理临时数据
		removeUploadData(repository, uuid)
	}

	// 计算摘要
	calculatedDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))

	// 验证摘要
	if digest != calculatedDigest {
		log.Logger.Warn("Digest mismatch: expected ", digest, " but got ", calculatedDigest)
		return c.Status(fiber.StatusBadRequest).SendString("Digest mismatch")
	}

	// 重置文件指针
	if _, err := tempFile.Seek(0, 0); err != nil {
		log.Logger.Error("Failed to reset file pointer: ", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to process data")
	}

	// 获取文件大小
	fileInfo, err := tempFile.Stat()
	if err != nil {
		log.Logger.Error("Failed to get file info: ", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to process data")
	}
	fileSize := fileInfo.Size()

	// 使用流式方法存储blob
	if err := r.service.PutBlobFromReader(repository, digest, tempFile, fileSize); err != nil {
		log.Logger.Error("Failed to store blob: ", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to store blob")
	}

	c.Set("Docker-Content-Digest", digest)
	c.Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", repository, digest))
	return c.SendStatus(fiber.StatusCreated)
}

// 处理镜像清单拉取
func (r *RegistryAPI) handlePullManifest(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和引用
	repository := c.Locals("repository").(string)
	reference := c.Locals("reference").(string)

	log.Logger.Info("Pull manifest: ", repository, ":", reference, " (Method:", c.Method(), ")")

	manifest, contentType, err := r.service.GetManifest(repository, reference)
	if err != nil {
		log.Logger.Error("Manifest not found: ", err)
		return c.Status(fiber.StatusNotFound).SendString("Manifest not found")
	}

	// 计算清单的摘要
	digest := "sha256:" + utils.ComputeSHA256(manifest)

	// 设置必要的响应头
	c.Set("Content-Type", contentType)
	c.Set("Docker-Content-Digest", digest)
	c.Set("Content-Length", fmt.Sprintf("%d", len(manifest)))

	log.Logger.Info("Serving manifest ", repository, ":", reference, ", size: ", len(manifest),
		"bytes, digest: ", digest, ", content-type: ", contentType)

	// 如果是HEAD请求，不返回实际内容
	if c.Method() == "HEAD" {
		log.Logger.Info("HEAD request for manifest ", repository, ":", reference, " responding with headers only")
		return c.SendStatus(fiber.StatusOK)
	}

	// 对于GET请求，返回完整内容
	return c.Status(fiber.StatusOK).Send(manifest)
}

// 处理Blob拉取
func (r *RegistryAPI) handlePullBlob(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和摘要
	repository := c.Locals("repository").(string)
	digest := c.Locals("digest").(string)

	log.Logger.Info(c.Method(), " blob: ", repository, "@", digest)

	blob, err := r.service.GetBlob(repository, digest)
	if err != nil {
		log.Logger.Error("Blob not found: ", err)
		return c.Status(fiber.StatusNotFound).SendString("Blob not found")
	}

	c.Set("Content-Type", "application/octet-stream")
	c.Set("Docker-Content-Digest", digest)
	c.Set("Content-Length", fmt.Sprintf("%d", len(blob)))

	// 如果是HEAD请求，不返回实际内容
	if c.Method() == "HEAD" {
		log.Logger.Info("HEAD request for blob ", repository, "@", digest, "size: ", len(blob), " bytes")
		return c.SendStatus(fiber.StatusOK)
	}

	// 对于GET请求，返回完整内容
	log.Logger.Info("Serving blob ", repository, "@", digest, "size: ", len(blob), "bytes", repository, digest, len(blob))
	return c.Status(fiber.StatusOK).Send(blob)
}

// 处理镜像清单删除
func (r *RegistryAPI) handleDeleteManifest(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和引用
	repository := c.Locals("repository").(string)
	reference := c.Locals("reference").(string)

	log.Logger.Info("Delete manifest: ", repository, ":", reference)

	if err := r.service.DeleteManifest(repository, reference); err != nil {
		log.Logger.Error("Failed to delete manifest: ", err)
		return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to delete manifest: %v", err))
	}

	log.Logger.Info("Successfully deleted manifest ", repository, ":", reference)
	return c.SendStatus(fiber.StatusAccepted)
}

// 处理Blob删除
func (r *RegistryAPI) handleDeleteBlob(c *fiber.Ctx) error {
	// 从中间件中获取仓库名称和摘要
	repository := c.Locals("repository").(string)
	digest := c.Locals("digest").(string)

	log.Logger.Info("Delete blob: ", repository, "@", digest)

	if err := r.service.DeleteBlob(repository, digest); err != nil {
		log.Logger.Error("Failed to delete blob: ", err)

		// 如果blob仍被引用，返回409 Conflict
		if strings.Contains(err.Error(), "still referenced") {
			return c.Status(fiber.StatusConflict).SendString(fmt.Sprintf("Blob is still referenced by manifests: %v", err))
		}

		return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to delete blob: %v", err))
	}

	log.Logger.Info("Successfully deleted blob ", repository, "@", digest)
	return c.SendStatus(fiber.StatusAccepted)
}

// 垃圾回收处理函数
func (r *RegistryAPI) handleGarbageCollection(c *fiber.Ctx) error {
	log.Logger.Info("Starting garbage collection")

	// 获取所有blob
	blobs, err := r.service.ListBlobs()
	if err != nil {
		log.Logger.Error("Failed to list blobs: %v", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to list blobs")
	}

	deletedCount := 0

	// 检查每个blob是否被引用
	for _, blob := range blobs {
		parts := strings.Split(blob, ":")
		if len(parts) < 2 {
			continue
		}

		repository := parts[0]
		digest := strings.Join(parts[1:], ":")

		// 跳过分块数据
		if strings.Contains(digest, ":chunk:") {
			continue
		}

		refs, err := r.service.GetManifestReferences(digest)
		if err != nil {
			log.Logger.Error("Error checking references for %s: %v", blob, err)
			continue
		}

		// 如果没有引用，删除blob
		if len(refs) == 0 {
			log.Logger.Info("Deleting unreferenced blob: ", blob)
			if err := r.service.DeleteBlob(repository, digest); err != nil {
				log.Logger.Error("Failed to delete blob ", blob, ":", err)
			} else {
				deletedCount++
			}
		}
	}

	log.Logger.Info("Garbage collection completed: deleted ", deletedCount, " unreferenced blobs")

	return c.Status(fiber.StatusOK).JSON(map[string]interface{}{
		"deleted_blobs": deletedCount,
	})
}

// 添加一个调试API端点
func (r *RegistryAPI) handleListManifests(c *fiber.Ctx) error {
	manifests, err := r.service.ListManifests()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to list manifests: %v", err))
	}

	return c.Status(fiber.StatusOK).JSON(map[string]interface{}{
		"manifests": manifests,
	})
}
