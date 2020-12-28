package main

import (
	"context"
	"time"
	"path/filepath"
	"os"
	"io"

	"github.com/Gimulator/protobuf/go/api"
	"google.golang.org/grpc"
	"github.com/sirupsen/logrus"
	"github.com/Gimulator/master-logger/s3"
)

type Logger struct {
	messageClient api.MessageAPIClient
	userClient    api.UserAPIClient
	token         string	
	host          string
	log			  *logrus.Entry
	file		  *os.File
	path		  string
	filePath	  string
	roomID	  	  string	//is roomID int ?

}

func (l *Logger) CreateLogger() (*Logger, error) {
	err := l.getEnv()
	if err != nil {
		return nil, err
	}

	filePath= filepath.Join(l.path, l.roomID)

	l.file, err = os.Create(l.roomID)
	if err != nil {
		return nil, err
	}

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

	return nil
}

func (l *Logger) connect() error {
	for {
		conn, err := grpc.Dial(l.host)
		if err != nil {
			return err
		}

		l.messageClient = api.NewMessageAPIClient(conn)
		l.userClient = api.NewUserAPIClient(conn)

		if err == nil {
			break
		}
		print(err)
	}

	return nil
}

func (l *Logger) Watch() error {
	var timeout = time.Second * 100
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

	return nil
}

func (l *Logger) watchReceiver(stream api.MessageAPI_WatchClient) {
	for {
		mes, err := stream.Recv()
		if err != nil{
			return err
		}
		_, err = l.file.WriteString(fmt.Sprintf("%v\n", mes))
		if err == io.EOF {
			break
		}
		if err != nil {
			l.log.WithError(err).Error("error while receiving message")
			continue
		}
		if mes.Key.Type == api.Result {
			gzipit(l.filePath, l.path)
			l.log.Info("starting to upload ...")
			err := s3.Upload(logFile, l.roomID +".gz")
			if err != nil {
				l.log.WithError(err).Error("error while uploading")
				return err
			}
			return nil
		}

	return nil
}


func (l *Logger) appendMetadata(ctx context.Context) context.Context {
	data := make(map[string]string)
	data["token"] = l.token

	md := metadata.New(data)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx
}

func gzipit(source, target string) error {
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


func track()  error{
	l := &Logger{}
	l, err := l.CreateLogger()
	if err != nil {
		l.log.WithError(err).Error("could not create logger")
		return err
	}

	l.log.Info("starting to watch ...")
	err = l.Watch() 
	if err != nil {
		l.log.WithError(err).Error("error while watching")
		return err
	}

	return  nil
}


func main() {
	track() 	
}
