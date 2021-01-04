package logger

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	log           *logrus.Entry
	file          *os.File
	path          string
	filePath      string
	roomID        string
	Ch            chan string
}

func (l *Logger) NewLogger() (*Logger, error) {

	log := logrus.New()
	log.SetReportCaller(true)
	log.Formatter = &logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return fmt.Sprintf("%s()", f.Function), fmt.Sprintf("%s:%d", f.File, f.Line)
		}, //TODO: add logWithfield,search the best format
	}

	err := l.getEnv()
	if err != nil {
		return nil, err
	}
	//makes the logfile fullname by conjuncting directory and roomID
	l.filePath = l.path + "/" + l.roomID

	l.file, err = os.Create(l.roomID)
	if err != nil {
		return nil, err
	}

	if err := l.connect(); err != nil {
		return nil, err
	}

	l.Ch = make(chan string)

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

	return nil
}

func (l *Logger) connect() error {
	var waitTime = time.Second * 5
	for {
		conn, err := grpc.Dial(l.host)
		if err != nil {
			l.log.WithError(err).Error("could not connect to Gimulator, retrying")
			time.Sleep(waitTime)
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
	var timeout = time.Second * 10
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
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
	case l.Ch <- "end":
		close(l.Ch)
		return nil
	}
}

func (l *Logger) watchReceiver(stream api.MessageAPI_WatchClient) error {
	//resultType := &api.Result{}
	for {
		mes, err := stream.Recv()
		if err != nil {
			l.log.WithError(err).Error("could not receive the message to log, retrying ... ")
			continue
		}

		_, writeErr := l.file.WriteString(fmt.Sprintf("%v\n", mes))

		if writeErr != nil {
			l.log.WithError(err).Error("error while writing message to logfile")
		}
		if mes.Key.Type == "result" { //??
			gzipit(l.filePath, l.path)
			l.log.Info("starting to upload ...")
			err := s3.NewS3(l.path, l.roomID+".gz")
			if err != nil {
				l.log.WithError(err).Error("error while uploading")
				return err
			}
			l.Ch <- "end"
			return nil
		}
	}
}

func (l *Logger) appendMetadata(ctx context.Context) context.Context {
	data := make(map[string]string)
	data["token"] = l.token

	md := metadata.New(data)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx
}

func gzipit(source, target string) error { //TODO: move this func to a tools file or sth
	reader, err := os.Open(source)
	if err != nil {
		return err
	}

	filename := filepath.Base(source)
	target = filepath.Join(target, fmt.Sprintf("%s.gz", filename))
	writer, err := os.Create(target)
	if err != nil {
		return err
	}
	defer writer.Close()

	archiver := gzip.NewWriter(writer)
	archiver.Name = filename
	defer archiver.Close()

	_, err = io.Copy(archiver, reader)
	return err
}

func Track() error {
	l := &Logger{}

	l, err := l.NewLogger()

	if err != nil {
		return err
	}

	if err = l.Watch(); err != nil {
		return err
	}

	return nil
}
