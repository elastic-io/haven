package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

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

func (api *RegistryAPI) Init(c *config.SourceConfig) {
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

// 在 handleBlobUploadPatch 方法中添加错误处理
func (r *RegistryAPI) handleBlobUploadPatch(c *fiber.Ctx) error {
	startTime := time.Now()
	repository := c.Locals("repository").(string)
	uuid := c.Locals("uuid").(string)

	contentLength := c.Request().Header.ContentLength()
	if contentLength < 0 {
		contentLength = 0
	}

	log.Logger.Info("Patch blob: ", repository, " uuid: ", uuid)
	log.Logger.Debugf("[PATCH] Starting blob patch - repo: %s, uuid: %s, content-length: %d bytes",
		repository, uuid, contentLength)

	uploadData := getOrCreateUploadData(repository, uuid)
	if uploadData == nil {
		log.Logger.Error("[PATCH] Failed to create upload data")
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to create upload data")
	}

	initialSize := uploadData.Size()
	log.Logger.Debugf("[PATCH] Current upload size before patch: %d bytes", initialSize)

	// 处理数据
	if c.Request().IsBodyStream() && contentLength > 0 {
		log.Logger.Debugf("[PATCH] Processing streaming request, size: %d bytes", contentLength)

		bodyStream := c.Request().BodyStream()
		if bodyStream == nil {
			log.Logger.Error("[PATCH] Nil body stream received")
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		maxBytes := int64(c.App().Config().BodyLimit)
		safeReader := newSafeReader(bodyStream, maxBytes)

		// 添加进度监控
		progressReader := &progressReader{
			reader: safeReader,
			onProgress: func(bytesRead int64) {
				if bytesRead%types.MB == 0 { // 每1MB记录一次
					log.Logger.Debugf("[PATCH] Stream progress: %d MB read", bytesRead/types.MB)
				}
			},
		}

		streamStart := time.Now()
		if err := uploadData.AppendStream(progressReader); err != nil {
			log.Logger.Error("[PATCH] Failed to append stream data after %v: %v",
				time.Since(streamStart), err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
		log.Logger.Debugf("[PATCH] Stream processing completed in %v", time.Since(streamStart))

	} else if len(c.Body()) > 0 {
		log.Logger.Debugf("[PATCH] Processing non-stream body, size: %d bytes", len(c.Body()))

		bodyStart := time.Now()
		if err := uploadData.AppendData(c.Body()); err != nil {
			log.Logger.Error("[PATCH] Failed to append data after %v: %v",
				time.Since(bodyStart), err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
		log.Logger.Debugf("[PATCH] Body processing completed in %v", time.Since(bodyStart))
	}

	finalSize := uploadData.Size()
	bytesProcessed := finalSize - initialSize

	log.Logger.Debugf("[PATCH] Patch completed - processed: %d bytes, total size: %d bytes, duration: %v, speed: %.2f MB/s",
		bytesProcessed, finalSize, time.Since(startTime),
		float64(bytesProcessed)/types.MB/time.Since(startTime).Seconds())

	// 设置响应头
	if finalSize > 0 {
		c.Set("Range", fmt.Sprintf("0-%d", finalSize-1))
	} else {
		c.Set("Range", "0-0")
	}
	c.Set("Docker-Upload-UUID", uuid)
	c.Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, uuid))

	return c.SendStatus(fiber.StatusAccepted)
}

// 进度监控读取器
type progressReader struct {
	reader     io.Reader
	bytesRead  int64
	onProgress func(int64)
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	pr.bytesRead += int64(n)
	if pr.onProgress != nil {
		pr.onProgress(pr.bytesRead)
	}
	return n, err
}

// 改进的安全读取器包装器
type safeReader struct {
	r         io.Reader
	maxBytes  int64
	readBytes int64
}

func (sr *safeReader) Read(p []byte) (n int, err error) {
	if sr.r == nil {
		return 0, io.EOF
	}

	// 检查是否超过最大读取限制
	if sr.maxBytes > 0 && sr.readBytes >= sr.maxBytes {
		return 0, io.EOF
	}

	defer func() {
		if r := recover(); r != nil {
			log.Logger.Error("Recovered from panic in reader: ", r)
			err = fmt.Errorf("reader panic: %v", r)
			n = 0
		}
	}()

	// 限制单次读取大小
	if len(p) > 32*1024 { // 限制为32KB
		p = p[:32*1024]
	}

	// 如果设置了最大字节限制，确保不超过剩余可读字节数
	if sr.maxBytes > 0 {
		remaining := sr.maxBytes - sr.readBytes
		if int64(len(p)) > remaining {
			p = p[:remaining]
		}
	}

	n, err = sr.r.Read(p)
	sr.readBytes += int64(n)
	return n, err
}

// 创建安全读取器的辅助函数
func newSafeReader(r io.Reader, maxBytes int64) *safeReader {
	return &safeReader{
		r:         r,
		maxBytes:  maxBytes,
		readBytes: 0,
	}
}

// 处理Blob推送
func (r *RegistryAPI) handlePushBlob(c *fiber.Ctx) error {
	startTime := time.Now()
	repository := c.Locals("repository").(string)
	uuid := c.Locals("uuid").(string)
	digest := c.Query("digest")

	if digest == "" {
		return c.Status(fiber.StatusBadRequest).SendString("Missing digest parameter")
	}

	log.Logger.Info("Push blob: ", repository, " uuid: ", uuid, " digest: ", digest)
	log.Logger.Debugf("[PUT] Starting blob push - repo: %s, uuid: %s, digest: %s",
		repository, uuid, digest)

	// 创建临时文件
	tempFile, err := os.CreateTemp("", "blob-*")
	if err != nil {
		log.Logger.Errorf("[PUT] Failed to create temp file: %v", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to create temp file")
	}
	tempPath := tempFile.Name()
	defer func() {
		tempFile.Close()
		os.Remove(tempPath)
		log.Logger.Debugf("[PUT] Cleaned up temp file: %s", tempPath)
	}()

	hasher := sha256.New()
	multiWriter := io.MultiWriter(tempFile, hasher)

	var totalBytes int64
	contentLength := c.Request().Header.ContentLength()

	// 处理请求体或上传数据
	if contentLength > 0 {
		log.Logger.Debugf("[PUT] Processing request body, content-length: %d bytes", contentLength)

		if c.Request().IsBodyStream() {
			log.Logger.Debug("[PUT] Processing streaming request body")

			maxBytes := int64(c.App().Config().BodyLimit)
			safeReader := newSafeReader(c.Request().BodyStream(), maxBytes)

			// 添加进度监控
			progressReader := &progressReader{
				reader: safeReader,
				onProgress: func(bytesRead int64) {
					if bytesRead%(10*types.MB) == 0 { // 每10MB记录一次
						log.Logger.Debugf("[PUT] Processing progress: %d MB", bytesRead/types.MB)
					}
				},
			}

			copyStart := time.Now()
			written, err := io.Copy(multiWriter, progressReader)
			if err != nil {
				log.Logger.Errorf("[PUT] Failed to process request stream after %v: %v",
					time.Since(copyStart), err)
				return c.Status(fiber.StatusInternalServerError).SendString("Failed to process request body")
			}
			totalBytes = written
			log.Logger.Debugf("[PUT] Stream copy completed: %d bytes in %v, speed: %.2f MB/s",
				written, time.Since(copyStart), float64(written)/types.MB/time.Since(copyStart).Seconds())
		} else {
			writeStart := time.Now()
			bodyData := c.Body()
			if _, err := multiWriter.Write(bodyData); err != nil {
				log.Logger.Errorf("[PUT] Failed to write request body: %v", err)
				return c.Status(fiber.StatusInternalServerError).SendString("Failed to process request body")
			}
			totalBytes = int64(len(bodyData))
			log.Logger.Debugf("[PUT] Body write completed: %d bytes in %v",
				totalBytes, time.Since(writeStart))
		}
	} else {
		log.Logger.Debug("[PUT] Processing upload data from PATCH requests")

		uploadData := getUploadData(repository, uuid)
		if uploadData == nil {
			log.Logger.Error("[PUT] No upload data found")
			return c.Status(fiber.StatusNotFound).SendString("No upload data found")
		}

		uploadSize := uploadData.Size()
		log.Logger.Debugf("[PUT] Upload data size: %d bytes", uploadSize)

		writeStart := time.Now()
		if err := uploadData.WriteTo(multiWriter); err != nil {
			log.Logger.Errorf("[PUT] Failed to process upload data after %v: %v",
				time.Since(writeStart), err)
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to process upload data")
		}
		totalBytes = uploadSize
		log.Logger.Debugf("[PUT] Upload data write completed: %d bytes in %v, speed: %.2f MB/s",
			totalBytes, time.Since(writeStart), float64(totalBytes)/types.MB/time.Since(writeStart).Seconds())

		removeUploadData(repository, uuid)
		log.Logger.Debug("[PUT] Cleaned up upload data")
	}

	// 验证摘要
	calculatedDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if digest != calculatedDigest {
		log.Logger.Warnf("[PUT] Digest mismatch: expected %s but got %s", digest, calculatedDigest)
		return c.Status(fiber.StatusBadRequest).SendString("Digest mismatch")
	}
	log.Logger.Debugf("[PUT] Digest verification passed: %s", digest)

	// 重置文件指针并获取文件信息
	if _, err := tempFile.Seek(0, 0); err != nil {
		log.Logger.Errorf("[PUT] Failed to reset file pointer: %v", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to process data")
	}

	fileInfo, err := tempFile.Stat()
	if err != nil {
		log.Logger.Errorf("[PUT] Failed to get file info: %v", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to process data")
	}
	fileSize := fileInfo.Size()

	// 存储blob
	log.Logger.Debugf("[PUT] Starting blob storage: %d bytes", fileSize)
	storeStart := time.Now()

	if err := r.service.PutBlobFromReader(repository, digest, tempFile, fileSize); err != nil {
		log.Logger.Errorf("[PUT] Failed to store blob after %v: %v",
			time.Since(storeStart), err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to store blob")
	}

	storeTime := time.Since(storeStart)
	totalTime := time.Since(startTime)

	log.Logger.Infof("Successfully stored blob %s: %d bytes in %v", digest, fileSize, totalTime)
	log.Logger.Debugf("[PUT] Blob push completed - total: %d bytes, store time: %v, total time: %v, store speed: %.2f MB/s, overall speed: %.2f MB/s",
		fileSize, storeTime, totalTime,
		float64(fileSize)/types.MB/storeTime.Seconds(),
		float64(fileSize)/types.MB/totalTime.Seconds())

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
