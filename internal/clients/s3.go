package clients

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Client interface {
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]string, error)

	ListObjectsInBucket(bucket string) ([]string, error)
	// ListObjectsInBucketDir dir是目标文件夹路径,需要以"/"结尾
	ListObjectsInBucketDir(bucket, dir string) ([]string, error)

	UploadObject(bucket, objectKey string, body []byte) error
	UploadObjectStream(bucket, objectKey, filePath string) error
	DownloadObject(bucket, objectKey string) ([]byte, error)
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string) error
	DeleteObject(bucket, objectKey string) error

	GeneratePresignedURL(bucket, objectKey string, expiresIn int64) (string, error)
	IsBucketPubliclyReadable(bucket string) (bool, error)
	IsBucketPubliclyWritable(bucket string) (bool, error)
}

type s3Client struct {
	Client    *s3.S3
	Region    string
	EndPoint  string
	AccessKey string
	SecretKey string
	Token     string
}

type S3Options struct {
	S3ForcePathStyle   bool
	DisableSSL         bool
	InsecureSkipVerify bool
}

func News3Client(ak, sk, token, region, endpoint string, opts ...S3Options) (S3Client, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       &http.Client{},
		Credentials:      credentials.NewStaticCredentials(ak, sk, token),
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize S3 client, reason: %s", err.Error())
	}
	if len(opts) > 0 {
		if v1 := opts[0].S3ForcePathStyle; v1 {
			sess.Config.S3ForcePathStyle = &v1
		}
		if v2 := opts[0].DisableSSL; v2 {
			sess.Config.DisableSSL = &v2
		}
		if v3 := opts[0].InsecureSkipVerify; v3 {
			sess.Config.HTTPClient.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: v3,
				},
			}
		}
	}
	return &s3Client{
		Client:    s3.New(sess),
		Region:    region,
		EndPoint:  endpoint,
		AccessKey: ak,
		SecretKey: sk,
		Token:     token,
	}, nil
}

// UploadObject objectKey是所上传文件在bucket中的相对路径(需包含文件名称本身)
func (s *s3Client) UploadObject(bucket, objectKey string, body []byte) error {
	uploader := s3manager.NewUploaderWithClient(s.Client)
	fileReader := bytes.NewReader(body)
	input := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   fileReader,
	}
	_, err := uploader.Upload(input)
	if err != nil {
		return err
	}
	return nil
}

// UploadObject4HugeFile 针对大文件使用的流式上传
func (s *s3Client) UploadObjectStream(bucket, objectKey, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	uploader := s3manager.NewUploaderWithClient(s.Client)
	input := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	}
	_, err = uploader.Upload(input)
	if err != nil {
		return err
	}

	return nil
}

func (s *s3Client) DownloadObject(bucket, objectKey string) ([]byte, error) {
	downloader := s3manager.NewDownloaderWithClient(s.Client)
	buffer := &aws.WriteAtBuffer{}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	}
	_, err := downloader.Download(buffer, input)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *s3Client) CopyObject(sourceBucket, sourceKey, destinationBucket, destinationKey string) error {
	input := &s3.CopyObjectInput{
		CopySource: aws.String(sourceBucket + "/" + sourceKey),
		Bucket:     aws.String(destinationBucket),
		Key:        aws.String(destinationKey),
	}
	_, err := s.Client.CopyObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Client) DeleteObject(bucket, objectKey string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	}
	_, err := s.Client.DeleteObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Client) CreateBucket(bucket string) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	if s.bucketExists(bucket) {
		return nil
	}

	_, err := s.Client.CreateBucket(input)
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Client) bucketExists(bucket string) bool {
	buckets, _ := s.ListBuckets()
	for _, bucketExisted := range buckets {
		if bucket == bucketExisted {
			return true
		}
	}
	return false
}

func (s *s3Client) DeleteBucket(bucket string) error {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}
	if !s.bucketExists(bucket) {
		return nil
	}
	_, err := s.Client.DeleteBucket(input)
	return err
}

func (s *s3Client) ListBuckets() ([]string, error) {
	results, err := s.Client.ListBuckets(nil)
	if err != nil {
		return nil, err
	}
	buckets := make([]string, 0)
	for _, b := range results.Buckets {
		buckets = append(buckets, *b.Name)
	}
	return buckets, nil
}

func (s *s3Client) ListObjectsInBucket(bucket string) ([]string, error) {
	input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	results, err := s.Client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}
	objects := make([]string, 0)
	for _, item := range results.Contents {
		objects = append(objects, *item.Key)
	}
	return objects, nil
}

func (s *s3Client) ListObjectsInBucketDir(bucket, dir string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(dir),
	}
	results, err := s.Client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}
	objects := make([]string, 0)
	for _, item := range results.Contents {
		objects = append(objects, *item.Key)
	}
	return objects, nil
}

// GeneratePresignedURL 生成一个预签名URL，用于下载S3对象(默认遵循aws signature version 4)
func (s *s3Client) GeneratePresignedURL(bucket, objectKey string, expiresIn int64) (string, error) {
	pulicReadable, err := s.IsBucketPubliclyReadable(bucket)
	if err != nil {
		return "", fmt.Errorf("failed to judge bucket readable: %v", err)
	}
	//公有读bucket外链
	if pulicReadable {
		endPoint := strings.Replace(s.EndPoint, "s3-internal", "s3", 1)
		return fmt.Sprintf("https://%s.%s/%s", bucket, endPoint, objectKey), nil
	}
	//非公有读bucket外链
	req, _ := s.Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	urlStr, err := req.Presign(time.Duration(expiresIn) * time.Second)
	if err != nil {
		return "", fmt.Errorf("failed to sign request: %v", err)
	}
	return urlStr, nil
}

func (s *s3Client) IsBucketPubliclyReadable(bucket string) (bool, error) {
	result, err := s.Client.GetBucketAcl(&s3.GetBucketAclInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return false, fmt.Errorf("无法获取bucket ACL: %v", err)
	}

	for _, grant := range result.Grants {
		if *grant.Grantee.Type == "Group" {
			if *grant.Permission == "READ" {
				return true, nil
			}
		}
	}

	return false, nil
}

func (s *s3Client) IsBucketPubliclyWritable(bucket string) (bool, error) {
	result, err := s.Client.GetBucketAcl(&s3.GetBucketAclInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return false, fmt.Errorf("无法获取bucket ACL: %v", err)
	}

	for _, grant := range result.Grants {
		if *grant.Grantee.Type == "Group" {
			if *grant.Permission == "WRITE" {
				return true, nil
			}
		}
	}

	return false, nil
}
