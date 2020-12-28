package s3

import (
	"fmt"
	"os"

	minio "github.com/minio/minio-go/v6"
)

type S3 struct {
	*minio.Client

	endpoint        string
	bucketName      string
	accessKeyID     string
	secretAccessKey string
}

func Upload(filePath, fileName string) error {
	s := &S3{}
	if err := s.getEnv(); err != nil {
		return err
	}
	var err error
	s.Client, err = minio.New(s.endpoint, s.accessKeyID, s.secretAccessKey, false)
	if err != nil {
		return err
	}
	_, puterr := s.FPutObject(s.bucketName, fileName, filePath, minio.PutObjectOptions{})
	if puterr != nil {
		return puterr
	}
	return nil
}

func (s *S3) getEnv() error {
	s.endpoint = os.Getenv("MASTER_S3_URL")

	s.bucketName = os.Getenv("MASTER_S3_BUCKET")

	s.accessKeyID = os.Getenv("MASTER_S3_ACCESS_KEY")

	s.secretAccessKey = os.Getenv("MASTER_S3_SECRET_KEY")

	if s.endpoint == "" || s.bucketName == "" || s.accessKeyID == "" || s.secretAccessKey == "" {
		return fmt.Errorf("s3 credential(s) are not set in environment ")
	}

	return nil
}
