// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"errors"
	"flag"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Variables configured at program start from program parameters and other inputs
var (
	sourceBucket, sourcePrefix, sourceBucketPrefix string
	targetBucket, targetPrefix, targetBucketPrefix string
	endpoint, accessKey, secretKey                 string
	enableCleanUp                                  string

	targetObject                       string
	downloadDirectory, uploadDirectory string

	// Debug
	objectCount, objectSize, fileCount, fileSize int64
)

const (
	// ContentType is defaulted to application/octet-stream for this demo
	ContentType = "application/octet-stream"
	// StagingDirectory is the directory when the append operation is performed
	StagingDirectory = "/tmp"
)

func main() {
	flag.StringVar(&sourceBucketPrefix, "source-bucket-prefix", "", "s3 source containing miscellaneous objects")
	flag.StringVar(&targetBucketPrefix, "target-bucket-prefix", "", "s3 target receiving single resulting object")

	flag.StringVar(&endpoint, "endpoint", "", "s3 endpoint with config")
	flag.StringVar(&accessKey, "accesskey", "", "access key of s3 endpoint with config")
	flag.StringVar(&secretKey, "secretkey", "", "secret key of s3 endpoint with config")

	flag.StringVar(&enableCleanUp, "enable-clean-up", "false", "delete debugging staging directories")

	flag.Parse()

	// Parse buckets and prefixes
	log.Println("Source Bucket/Prefix:", sourceBucketPrefix)
	log.Println("Target Bucket/Prefix:", targetBucketPrefix)
	if len(strings.SplitN(sourceBucketPrefix, "/", 2)) != 2 {
		log.Fatalln("source-bucket-prefix must contain a bucket and prefix")
	}
	if len(strings.SplitN(targetBucketPrefix, "/", 2)) != 2 {
		log.Fatalln("target-bucket-prefix must contain a bucket and prefix")
	}
	sourceBucket = strings.SplitN(sourceBucketPrefix, "/", 2)[0]
	sourcePrefix = strings.SplitN(sourceBucketPrefix, "/", 2)[1]
	targetBucket = strings.SplitN(targetBucketPrefix, "/", 2)[0]
	targetPrefix = strings.SplitN(targetBucketPrefix, "/", 2)[1]

	// Connect to minio
	s3Client, err := createClient(endpoint)
	if err != nil {
		log.Printf("Failed to create minio client %v\n", err)
	}

	ctx := context.Background()
	now := time.Now().Unix()
	downloadDirectory = strings.Join([]string{StagingDirectory, sourceBucket, strconv.Itoa(int(now))}, "/")
	uploadDirectory = strings.Join([]string{StagingDirectory, targetBucket, strconv.Itoa(int(now))}, "/")
	targetObject = strings.Join([]string{sourceBucket, strconv.Itoa(int(now))}, "-")
	log.Printf("Staging Directory: %v", downloadDirectory)

	// Download objects to staging directory
	err = downloadObjects(ctx, s3Client)
	if err != nil {
		cleanUp()
		return
	}

	// Append all source objects into a single resulting target object
	err = appendObjects()
	if err != nil {
		cleanUp()
		return
	}

	// Upload single resulting object
	err = uploadObject(ctx, s3Client)
	if err != nil {
		cleanUp()
		return
	}

	cleanUp()
}

// Create a minio client
func createClient(configEndpoint string) (*minio.Client, error) {
	s3Client, err := minio.New(configEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	return s3Client, nil
}

func downloadObjects(ctx context.Context, s3Client *minio.Client) error {
	if err := os.MkdirAll(filepath.Dir(downloadDirectory), 0777); err != nil {
		log.Printf("Failed to make directory: %v - %v\n", downloadDirectory, err)
		return err
	}

	opts := minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    sourcePrefix,
	}

	// List all objects from a bucket-name with a matching prefix.
	for object := range s3Client.ListObjects(ctx, sourceBucket, opts) {
		if object.Err != nil {
			log.Printf("Failed to list: %v - %v\n", object.Key, object.Err)
			return object.Err
		} else {
			objectCount++
			objectName := strings.Join([]string{downloadDirectory, object.Key}, "/")
			log.Printf("Downloading: %v to %v", object.Key, objectName)
			if err := s3Client.FGetObject(context.Background(), sourceBucket /*bucketName*/, object.Key /*objectName*/, objectName /*objectName*/, minio.GetObjectOptions{}); err != nil {
				log.Printf("Failed to download file: %v - %v\n", objectName, err)
				return err
			}
			objectSize += object.Size
		}
	}
	if objectCount == 0 {
		log.Println("Failed to find objects - exiting")
		return errors.New("no objects found")
	}
	log.Printf("Found objects: %v, size: %v", objectCount, objectSize)

	return nil
}

func appendObjects() error {
	// Allows for mutual exclusion
	var mu sync.Mutex
	// Remove file if exists
	objectName := strings.Join([]string{uploadDirectory, targetObject}, "/")
	err := os.Remove(objectName)
	if err != nil {
		// Could not remove file
		log.Printf("Failed to remove file: %v - %v\n", objectName, err)
	}
	if err := os.MkdirAll(filepath.Dir(objectName), 0777); err != nil {
		log.Printf("Failed to make directory: %v - %v\n", objectName, err)
		return err
	}
	out, err := os.OpenFile(objectName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		log.Printf("Failed to open file: %v - %v\n", objectName, err)
		return err
	}
	defer out.Close()
	// Locking objects to prevent other processes from modifying them
	mu.Lock()
	// Walk the file path to append all files
	err = filepath.Walk(downloadDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			mu.Unlock()
			log.Printf("Failed to walk: %v\n", err)
			return err
		}
		if !info.IsDir() {
			fileCount++
			log.Printf("Adding: %s\n", info.Name())
			in, err := os.Open(path)
			if err != nil {
				mu.Unlock()
				log.Printf("Failed to open: %v - %v\n", path, err)
				return err
			}
			_, err = io.Copy(out, in)
			if err != nil {
				mu.Unlock()
				log.Printf("Failed to append: %v - %v\n", path, err)
				return err
			}
			info, err := in.Stat()
			if err != nil {
				mu.Unlock()
				log.Printf("Failed to stat: %v - %v\n", path, err)
				return err
			}
			fileSize += info.Size()
		}
		return nil
	})
	log.Printf("Processed files: %v, size: %v", fileCount, fileSize)
	info, err := out.Stat()
	if err != nil {
		mu.Unlock()
		log.Printf("Failed to stat: %v - %v\n", out.Name(), err)
		return err
	}
	log.Printf("Created resulting file: %v, size: %v", out.Name(), info.Size())

	if err != nil {
		log.Printf("Could not walk: %v - %v\n", downloadDirectory, err)
		return err
	}
	// Test sum of objects versus files
	if objectCount != fileCount {
		mu.Unlock()
		log.Printf("Found total files number: %v - expected %v\n", fileCount, objectCount)
		return err
	}

	// Test cumulative object sizes versus file sizes
	if objectSize != fileSize {
		mu.Unlock()
		log.Printf("Found total files size: %v - expected: %v\n", fileSize, objectSize)
		return err
	}

	// Test resulting file size
	if objectSize != info.Size() {
		mu.Unlock()
		log.Printf("Found resulting object size: %v - expected: %v\n", info.Size(), objectSize)
		return err
	}

	mu.Unlock()
	return nil
}

func uploadObject(ctx context.Context, s3Client *minio.Client) error {
	// Make a new bucket if it does not exist
	opts := minio.MakeBucketOptions{}
	err := s3Client.MakeBucket(ctx, targetBucket, opts)
	if err != nil {
		// Check to see if we already own this bucket
		exists, err := s3Client.BucketExists(ctx, targetBucket)
		if err == nil && exists {
			// If bucket already exists and owned then continue
			log.Printf("Bucket already exists: %s\n", targetBucket)
		} else if err != nil {
			log.Printf("Failed to check if bucket exists: %s - %v", targetBucket, err)
			return err
		}
	} else {
		log.Printf("Successfully created bucket %s\n", targetBucket)
	}

	// Upload the object
	log.Printf("Uploading %s to %s\n", targetObject, targetBucketPrefix)
	_, err = s3Client.FPutObject(ctx, targetBucket /*bucketName*/, strings.Join([]string{targetPrefix, targetObject}, "/") /*objectName*/, strings.Join([]string{uploadDirectory, targetObject}, "/") /*filePath*/, minio.PutObjectOptions{ContentType: ContentType})
	if err != nil {
		log.Printf("Failed to upload object %v - %v\n", strings.Join([]string{targetPrefix, targetObject}, "/"), err)
		return err
	}

	log.Printf("Successfully uploaded %s to %s\n", targetObject, targetBucketPrefix)
	return nil
}

func cleanUp() {
	if enableCleanUp != "true" {
		if err := os.RemoveAll(filepath.Dir(downloadDirectory)); err != nil {
			log.Fatalf("Failed to cleanup directory: %v - %v\n", downloadDirectory, err)
		}
		if err := os.RemoveAll(filepath.Dir(uploadDirectory)); err != nil {
			log.Fatalf("Failed to cleanup directory: %v - %v\n", downloadDirectory, err)
		}
		log.Println("Staging directories cleaned up")
		return
	}
	log.Println("Clean up is disabled")
}
