package s3

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cheggaaa/pb"
	minio "github.com/minio/minio-go/v6"
	"github.com/sirupsen/logrus"
)

var timeout = time.Second * 120

type S3 struct {
	*minio.Client

	endpoint         string
	bucketName       string
	accessKeyID      string
	secretAccessKey  string
	log              *logrus.Entry
	IsUploadFinished bool
	Progress         *pb.ProgressBar
}

func NewS3(filePath, fileName string) error {
	s := &S3{}
	if err := s.getEnv(); err != nil {
		return err
	}
	var err error
	for i := 1; i < 4; i++ {
		s.Client, err = minio.New(s.endpoint, s.accessKeyID, s.secretAccessKey, false)
		if err != nil {
			s.log.WithError(err).Error("could not connect to S3, retrying ")
			continue
		}
		s.log.Info("successfully connected to S3")

	}

	reader, fileSize := s.NewProgressBar(fileName)

	err = s.Upload(fileName, filePath, reader, fileSize)
	return err
}

func (s *S3) Upload(fileName, filePath string, reader *minio.Object, fileSize int64) error {
	var waitTime = time.Second * 20
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, _ = s.PutObjectWithContext(ctx, s.bucketName, fileName, reader, fileSize, minio.PutObjectOptions{})

	for {
		if s.Progress.IsFinished() {
			break
		}
		time.Sleep(waitTime)
	}
	s.log.Info("successfully uploaded logfile to S3")

	return nil
}

func (s S3) NewProgressBar(fileName string) (*minio.Object, int64) {
	reader, err := s.GetObject(s.bucketName, fileName, minio.GetObjectOptions{})
	if err != nil {
		s.log.WithError(err).Error("could not get object while setting up the reader ")
	}
	defer reader.Close()

	objectInfo, err := reader.Stat()
	if err != nil {
		s.log.Fatalln(err)
	}

	// Progress reader is notified as PutObject makes progress with
	// the Reads inside.
	progress := pb.New64(objectInfo.Size)
	progress.Start()
	//s.IsUploadFinished = progress.IsFinished()
	return reader, objectInfo.Size
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
