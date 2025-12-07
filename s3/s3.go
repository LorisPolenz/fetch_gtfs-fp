package s3

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func initS3Client(endpoint string, accessKey string, secretKey string, useSSL bool) *minio.Client {

	// Initialize minio client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		panic(err)
	}

	return minioClient
}

func CheckS3ObjectExists(endpoint, accessKey, secretKey, bucketName, objectName string) (bool, error) {
	minioClient := initS3Client(endpoint, accessKey, secretKey, true)

	_, err := minioClient.StatObject(context.Background(), bucketName, objectName, minio.StatObjectOptions{})

	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err // Some other error occurred
	}
	return true, nil // Object exists
}

func PushS3Object(endpoint, accessKey, secretKey, bucketName, objectName string, data []byte) error {
	minioClient := initS3Client(endpoint, accessKey, secretKey, true)

	// Upload the object
	_, err := minioClient.PutObject(context.Background(), bucketName, objectName, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	return err
}
