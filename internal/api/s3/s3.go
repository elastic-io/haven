package s3

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"strconv"

	"github.com/elastic-io/haven/internal/api"
	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/service"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/gofiber/fiber/v2"
)

func init() {
	API := S3API{name: "s3"}
	api.APIRegister(API.name, &API)
}

type S3API struct {
	name    string
	service service.S3Service
}

// S3 API 响应结构
type ListBucketResult struct {
	XMLName        xml.Name         `xml:"ListBucketResult"`
	Name           string           `xml:"Name"`
	Prefix         string           `xml:"Prefix"`
	Marker         string           `xml:"Marker"`
	MaxKeys        int              `xml:"MaxKeys"`
	IsTruncated    bool             `xml:"IsTruncated"`
	Contents       []S3Object       `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefixes `xml:"CommonPrefixes,omitempty"`
}

type S3Object struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

type CommonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

func (api *S3API) Init(c *config.Config) {
	var err error
	api.service, err = service.NewS3Service(c.Storage)
	if err != nil {
		panic(fmt.Errorf(err.Error()))
	}
}

func (api *S3API) RegisterRoutes(app *fiber.App) {
	log.Logger.Info("Registering S3 compatible API routes")

	// 创建一个S3 API组
	s3 := app.Group("/s3")

	// S3 API 基础路由 - 列出所有存储桶
	s3.Get("/", api.handleS3Base)

	// 存储桶操作
	s3.Put("/:bucket", api.handleS3Bucket)
	s3.Delete("/:bucket", api.handleS3DeleteBucket)
	s3.Get("/:bucket/", api.handleS3ListBucket)

	// 对象操作
	s3.Get("/:bucket/:key", api.handleS3GetObject)
	s3.Head("/:bucket/:key", api.handleS3GetObject) // 使用相同的处理函数处理HEAD请求
	s3.Put("/:bucket/:key", api.handleS3PutObject)
	s3.Delete("/:bucket/:key", api.handleS3DeleteObject)

	// 分段上传操作 - 使用查询参数
    // 分段上传操作 - 使用查询参数
    s3.Post("/:bucket/:key", func(c *fiber.Ctx) error {
    if c.Query("uploads") != "" {
        return api.handleS3CreateMultipartUpload(c)
    }
    if c.Query("uploadId") != "" && c.Query("uploads") == "" {
        return api.handleS3CompleteMultipartUpload(c)
    }
    return c.Next()
    })

    s3.Put("/:bucket/:key", func(c *fiber.Ctx) error {
    if c.Query("partNumber") != "" && c.Query("uploadId") != "" {
        return api.handleS3UploadPart(c)
    }
    return api.handleS3PutObject(c)
    })

    s3.Delete("/:bucket/:key", func(c *fiber.Ctx) error {
    if c.Query("uploadId") != "" {
        return api.handleS3AbortMultipartUpload(c)
    }
    return api.handleS3DeleteObject(c)
    })
}

// 处理 S3 API 基础路由
func (api *S3API) handleS3Base(c *fiber.Ctx) error {
	log.Logger.Info("S3 API base request: %s %s", c.Method(), c.Path())

	// 列出所有存储桶
	buckets, err := api.service.ListBuckets()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to list buckets")
	}

	// 构建 XML 响应
	type ListAllMyBucketsResult struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Owner   struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		} `xml:"Owner"`
		Buckets struct {
			Bucket []struct {
				Name         string    `xml:"Name"`
				CreationDate time.Time `xml:"CreationDate"`
			} `xml:"Bucket"`
		} `xml:"Buckets"`
	}

	result := ListAllMyBucketsResult{}
	result.Owner.ID = "s3compatible"
	result.Owner.DisplayName = "S3 Compatible Storage"

	for _, bucket := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, struct {
			Name         string    `xml:"Name"`
			CreationDate time.Time `xml:"CreationDate"`
		}{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		})
	}

	c.Set("Content-Type", "application/xml")
	xmlData, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate response")
	}

	return c.Status(fiber.StatusOK).Send(xmlData)
}

// 处理创建存储桶请求
func (api *S3API) handleS3Bucket(c *fiber.Ctx) error {
	bucket := c.Params("bucket")

	log.Logger.Info("S3 create bucket request: %s", bucket)

	err := api.service.CreateBucket(bucket)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return c.Status(fiber.StatusConflict).SendString("Bucket already exists")
		} else {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to create bucket")
		}
	}

	return c.SendStatus(fiber.StatusOK)
}

// 处理删除存储桶请求
func (api *S3API) handleS3DeleteBucket(c *fiber.Ctx) error {
	bucket := c.Params("bucket")

	log.Logger.Info("S3 delete bucket request: %s", bucket)

	err := api.service.DeleteBucket(bucket)
	if err != nil {
		if strings.Contains(err.Error(), "not empty") {
			return c.Status(fiber.StatusConflict).SendString("The bucket you tried to delete is not empty")
		} else if strings.Contains(err.Error(), "not found") {
			return c.Status(fiber.StatusNotFound).SendString("No such bucket")
		} else {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to delete bucket")
		}
	}

	return c.SendStatus(fiber.StatusNoContent)
}

// 处理 S3 存储桶列表请求
func (api *S3API) handleS3ListBucket(c *fiber.Ctx) error {
	bucket := c.Params("bucket")

	log.Logger.Info("S3 list bucket request: %s", bucket)

	// 获取查询参数
	prefix := c.Query("prefix")
	marker := c.Query("marker")
	maxKeys := 1000 // 默认值
	delimiter := c.Query("delimiter")

	// 检查存储桶是否存在
	exists, err := api.service.BucketExists(bucket)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to check bucket")
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).SendString("No such bucket")
	}

	// 使用扩展的存储接口列出对象
	objects, commonPrefixes, err := api.service.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to list objects")
	}

	// 构建 S3 对象列表
	var bucketObjects []S3Object
	for _, obj := range objects {
		bucketObjects = append(bucketObjects, S3Object{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	// 构建响应
	result := ListBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: len(bucketObjects) >= maxKeys,
		Contents:    bucketObjects,
	}

	// 如果使用了分隔符，添加公共前缀
	if delimiter != "" && len(commonPrefixes) > 0 {
		for _, prefix := range commonPrefixes {
			result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefixes{
				Prefix: prefix,
			})
		}
	}

	c.Set("Content-Type", "application/xml")
	xmlData, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate response")
	}

	return c.Status(fiber.StatusOK).Send(xmlData)
}

// 处理 S3 对象获取请求
func (api *S3API) handleS3GetObject(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")

	log.Logger.Info("S3 get object request: %s/%s", bucket, key)

	// 检查存储桶是否存在
	exists, err := api.service.BucketExists(bucket)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to check bucket")
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).SendString("No such bucket")
	}

	// 获取对象
	object, err := api.service.GetObject(bucket, key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.Status(fiber.StatusNotFound).SendString("No such key")
		} else {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to get object")
		}
	}

	// 设置响应头
	c.Set("Content-Type", object.ContentType)
	c.Set("Content-Length", fmt.Sprintf("%d", len(object.Data)))
	c.Set("ETag", object.ETag)
	c.Set("Last-Modified", object.LastModified.Format(fiber.MIMEApplicationJSON))

	// 设置用户元数据
	for k, v := range object.Metadata {
		c.Set("x-amz-meta-"+k, v)
	}

	// 如果是 HEAD 请求，不返回内容
	if c.Method() == "HEAD" {
		return c.SendStatus(fiber.StatusOK)
	}

	return c.Status(fiber.StatusOK).Send(object.Data)
}

// 处理 S3 对象上传请求
func (api *S3API) handleS3PutObject(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")

	log.Logger.Info("S3 put object request: %s/%s", bucket, key)

	// 检查存储桶是否存在
	exists, err := api.service.BucketExists(bucket)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to check bucket")
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).SendString("No such bucket")
	}

	// 读取请求体
	data := c.Body()

	// 确定内容类型
	contentType := c.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// 提取用户元数据
	metadata := make(map[string]string)
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := string(key)
		v := string(value)
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
			metadata[metaKey] = v
		}
	})

	// 创建对象
	object := service.S3ObjectData{
		Key:          key,
		Data:         data,
		ContentType:  contentType,
		LastModified: time.Now(),
		ETag:         "\"" + utils.ComputeMD5(data) + "\"",
		Metadata:     metadata,
	}

	// 存储对象
	err = api.service.PutObject(bucket, object)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to store object")
	}

	c.Set("ETag", object.ETag)
	return c.SendStatus(fiber.StatusOK)
}

// 处理 S3 对象删除请求
func (api *S3API) handleS3DeleteObject(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")

	log.Logger.Info("S3 delete object request: %s/%s", bucket, key)

	// 检查存储桶是否存在
	exists, err := api.service.BucketExists(bucket)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to check bucket")
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).SendString("No such bucket")
	}

	// 删除对象
	err = api.service.DeleteObject(bucket, key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// S3 API 在删除不存在的对象时返回成功
			return c.SendStatus(fiber.StatusNoContent)
		}
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to delete object")
	}

	return c.SendStatus(fiber.StatusNoContent)
}

// 处理创建分段上传请求
func (api *S3API) handleS3CreateMultipartUpload(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")

	log.Logger.Info("S3 create multipart upload request: %s/%s", bucket, key)

	// 检查存储桶是否存在
	exists, err := api.service.BucketExists(bucket)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to check bucket")
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).SendString("No such bucket")
	}

	// 确定内容类型
	contentType := c.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// 提取用户元数据
	metadata := make(map[string]string)
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := string(key)
		v := string(value)
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
			metadata[metaKey] = v
		}
	})

	// 创建分段上传
	uploadID, err := api.service.CreateMultipartUpload(bucket, key, contentType, metadata)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to create multipart upload")
	}

	// 构建响应
	type InitiateMultipartUploadResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}

	result := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}

	c.Set("Content-Type", "application/xml")
	xmlData, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate response")
	}

	return c.Status(fiber.StatusOK).Send(xmlData)
}

// 处理上传分段请求
func (api *S3API) handleS3UploadPart(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")
	uploadID := c.Query("uploadId")
	partNumberStr := c.Query("partNumber")
	
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid part number")
	}

	log.Logger.Info("S3 upload part request: %s/%s, uploadID: %s, partNumber: %d", bucket, key, uploadID, partNumber)

	// 读取请求体
	data := c.Body()

	// 上传分段
	etag, err := api.service.UploadPart(bucket, key, uploadID, partNumber, data)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to upload part")
	}

	c.Set("ETag", etag)
	return c.SendStatus(fiber.StatusOK)
}

// 处理完成分段上传请求
func (api *S3API) handleS3CompleteMultipartUpload(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")
	uploadID := c.Query("uploadId")

	log.Logger.Info("S3 complete multipart upload request: %s/%s, uploadID: %s", bucket, key, uploadID)

	// 解析请求体
	var completeRequest struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}

	if err := xml.Unmarshal(c.Body(), &completeRequest); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Failed to parse request body")
	}

	// 构建分段信息
	var parts []service.MultipartPart
	for _, part := range completeRequest.Parts {
		parts = append(parts, service.MultipartPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}

	// 完成分段上传
	etag, err := api.service.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to complete multipart upload")
	}

	// 构建响应
	type CompleteMultipartUploadResult struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}

	result := CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/s3/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	}

	c.Set("Content-Type", "application/xml")
	xmlData, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to generate response")
	}

	return c.Status(fiber.StatusOK).Send(xmlData)
}

// 处理中止分段上传请求
func (api *S3API) handleS3AbortMultipartUpload(c *fiber.Ctx) error {
	bucket := c.Params("bucket")
	key := c.Params("key")
	uploadID := c.Query("uploadId")

	log.Logger.Info("S3 abort multipart upload request: %s/%s, uploadID: %s", bucket, key, uploadID)

	// 中止分段上传
	err := api.service.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to abort multipart upload")
	}

	return c.SendStatus(fiber.StatusNoContent)
}