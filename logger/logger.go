package logger

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Gimulator/master-logger/s3"
	"github.com/Gimulator/protobuf/go/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Logger struct {
	messageClient api.MessageAPIClient
	userClient    api.UserAPIClient
	token         string
	host          string
	endKey        string
	log           *logrus.Entry
	file          *os.File
	path          string
	filePath      string
	roomID        string
	Ch            chan string //TODO: do sth wiser about channel stuff
	ErrChan       chan error
}

type UploadResult struct {
	EndUpload string
	UploadErr error
}

func (l *Logger) NewLogger() (*Logger, error) {

	err := l.getEnv()
	if err != nil {
		return nil, err
	}
	//makes the logfile fullname by conjuncting directory and roomID, eg. /etc + / + 5623 + .log = /etc/5623.log
	l.filePath = l.path + "/" + l.roomID + ".log"

	l.file, err = os.Create(l.filePath)
	if err != nil {
		return nil, err
	}

	l.Ch = make(chan string, 0)
	l.ErrChan = make(chan error, 0)

	if err := l.connect(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Logger) getEnv() error {
	l.path = os.Getenv("MASTER_LOG_DIR")
	if l.path == "" {
		return fmt.Errorf("environment variable MASTER_LOG_DIR is not set")
	}

	l.roomID = os.Getenv("ROOM_ID")
	if l.roomID == "" {
		return fmt.Errorf("environment variable ROOM_ID is not set")
	}

	l.host = os.Getenv("GIMULATOR_HOST")
	if l.host == "" {
		return fmt.Errorf("environment variable GIMULATOR_HOST is not set")
	}

	l.token = os.Getenv("GIMULATOR_TOKEN")
	if l.token == "" {
		return fmt.Errorf("environment variable GIMULATOR_TOKEN is not set")
	}

	l.endKey = os.Getenv("END_ROOM_KEY")
	if l.endKey == "" {
		return fmt.Errorf("environment variable END_ROOM_KEY is not set")
	}
	return nil
}

func (l *Logger) connect() error {
	var dialWaitTime = time.Second * 5

	for i := 1; i < 4; i++ {

		conn, err := grpc.Dial(l.host)
		time.Sleep(dialWaitTime)

		if err != nil {
			l.log.WithError(err).Error("could not connect to Gimulator, retrying")

			continue
		}

		l.log.Info("successfully connected to Gimulator")

		l.messageClient = api.NewMessageAPIClient(conn)
		l.userClient = api.NewUserAPIClient(conn)

		break
	}
	return nil
}

func (l *Logger) Watch() error {
	var ctxTimeout = time.Second * 2
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	time.Sleep(ctxTimeout)
	ctx = l.appendMetadata(ctx)

	key := &api.Key{
		Type:      "",
		Name:      "",
		Namespace: "",
	}

	stream, err := l.messageClient.Watch(ctx, key)
	if err != nil {
		return err
	}

	go l.watchReceiver(stream)

	select {
	case res := <-l.Ch:
		close(l.Ch)
		close(l.ErrChan)
		println(res)
		return nil
	case err := <-l.ErrChan:
		close(l.ErrChan)
		close(l.Ch)
		return err
	}
}

func (l *Logger) watchReceiver(stream api.MessageAPI_WatchClient) error { //rename this func
	for {
		mes, err := stream.Recv()
		if err != nil {
			l.log.WithError(err).Error("could not receive this message, retrying ... ")
			continue
		}

		_, writeErr := l.file.WriteString(fmt.Sprintf("%v\n", mes))

		if writeErr != nil {
			l.log.WithError(err).Error("error while writing to logfile")
		}

		if mes.Key.Type == l.endKey {

			err := l.prepAndUpload()
			if err != nil {
				return err
			}
		}
	}
}

func (l *Logger) prepAndUpload() error { //rename this func
	err := gzipit(l.filePath, l.path)
	if err != nil {
		l.log.WithError(err).Error("error while zipping, starting to upload the unzipped logfile to s3")

		errCh := s3.NewS3(l.path, l.roomID+".log")
		if errCh != nil {
			l.log.WithError(errCh).Fatal("error occured while uploading logfile to s3")
			l.ErrChan <- errCh
			return errCh
		}

		l.Ch <- "end"
		return nil
	}

	l.log.Info("starting to upload to s3")
	errCh := s3.NewS3(l.path, l.roomID+".log.gz")
	if errCh != nil {
		l.log.WithError(errCh).Fatal("error occured while uploading logfile to s3")
		l.ErrChan <- errCh
		return errCh
	}

	l.Ch <- "end"
	return nil

}

func (l *Logger) appendMetadata(ctx context.Context) context.Context {
	data := make(map[string]string)
	data["token"] = l.token

	md := metadata.New(data)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx
}

func Track() error {
	l := &Logger{}

	l, err := l.NewLogger()

	if err != nil {
		return err
	}

	if err := l.Watch(); err != nil {
		return err
	}

	return nil
}
