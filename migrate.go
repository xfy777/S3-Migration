package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Source      S3Config `yaml:"source"`
	Destination S3Config `yaml:"destination"`
}

type S3Config struct {
	AccessKey        string `yaml:"accessKey"`
	SecretKey        string `yaml:"secretKey"`
	Endpoint         string `yaml:"endpoint"`
	Bucket           string `yaml:"bucket"`
	LocalDownloadPath string `yaml:"localDownloadPath"`
	Region           string `yaml:"region"`
}

func main() {
	configFile := "config.yaml"
	var config Config

	if err := readConfig(configFile, &config); err != nil {
		fmt.Println("Error reading config file:", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(config.Source.LocalDownloadPath, os.ModePerm); err != nil {
		fmt.Println("Error creating download directory:", err)
		os.Exit(1)
	}

	err := downloadFiles(config.Source.AccessKey, config.Source.SecretKey, config.Source.Endpoint, config.Source.Bucket, config.Source.LocalDownloadPath, config.Source.Region)
	if err != nil {
		fmt.Println("Error downloading files:", err)
		os.Exit(1)
	}

	err = uploadFiles(config.Destination.AccessKey, config.Destination.SecretKey, config.Destination.Endpoint, config.Destination.Bucket, config.Source.LocalDownloadPath, config.Destination.Region)
	if err != nil {
		fmt.Println("Error uploading files:", err)
		os.Exit(1)
	}

	err = os.RemoveAll(config.Source.LocalDownloadPath)
	if err != nil {
		fmt.Println("Error deleting files and directory:", err)
		os.Exit(1)
	}

	fmt.Println("Download and upload complete. Deleted local files and directory.")
}

func readConfig(file string, config *Config) error {
	yamlFile, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(yamlFile, config); err != nil {
		return err
	}

	return nil
}

func downloadFiles(accessKey, secretKey, endpoint, bucket, localPath, region string) error {
	config := &aws.Config{
		Endpoint:    aws.String(endpoint),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Region:      aws.String(region),
	}

	session := session.Must(session.NewSession(config))
	client := s3.New(session)

	listObjectsInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	listObjectsOutput, err := client.ListObjects(listObjectsInput)
	if err != nil {
		return err
	}

	for _, object := range listObjectsOutput.Contents {
		localFilePath := filepath.Join(localPath, *object.Key)

		if err := os.MkdirAll(filepath.Dir(localFilePath), os.ModePerm); err != nil {
			fmt.Println("Error creating subdirectories:", err)
			continue
		}

		getObjectInput := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    object.Key,
		}

		getObjectOutput, err := client.GetObject(getObjectInput)
		if err != nil {
			fmt.Println("Error getting object:", *object.Key, err)
			continue
		}

		if *object.Size > int64(0) {
			localFile, err := os.Create(localFilePath)
			if err != nil {
				fmt.Println("Error creating local file:", localFilePath, err)
				continue
			}
			defer localFile.Close()

			_, err = io.Copy(localFile, getObjectOutput.Body)
			if err != nil {
				fmt.Println("Error copying object to local file:", *object.Key, err)
				continue
			}
			fmt.Println("Downloaded file:", *object.Key)
		}
	}

	return nil
}

func uploadFiles(accessKey, secretKey, endpoint, bucket, directory, region string) error {
	config := &aws.Config{
		Endpoint:    aws.String(endpoint),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Region:      aws.String(region),
	}

	session := session.Must(session.NewSession(config))
	client := s3.New(session)

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		relPath, _ := filepath.Rel(directory, path)

		uploadKey := filepath.ToSlash(relPath)
		uploadKey = strings.TrimPrefix(uploadKey, "./")

		uploadInput := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(uploadKey),
			Body:   file,
		}

		if info.Size() > 0 {
			_, err = client.PutObject(uploadInput)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}
