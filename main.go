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
	"bytes"
	"context"
	"errors"
	"flag"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log"
	"strings"
	"time"
)

// Variables configured at program start from program parameters and other inputs
var (
	sourceBucket, sourcePrefix, sourceBucketPrefix string
	targetBucket, targetPrefix, targetBucketPrefix string
	endpoint, accessKey, secretKey                 string
	enableCleanUp                                  string

	buffer           *bytes.Buffer
	targetObjectName string

	// Debug
	objectCount, objectSize int64
)

const (
	// ContentType is defaulted to application/octet-stream for this demo
	ContentType = "application/octet-stream"
	// TimeFormat is the human-readable format used for file naming
	TimeFormat = "20060102150405"
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
	now := time.Now().UTC()
	targetObjectName = targetPrefix + "/" + sourceBucket + "-" + now.Format(TimeFormat)

	buffer = new(bytes.Buffer)

	// Download objects to memory
	err = downloadObjects(ctx, s3Client)
	if err != nil {
		return
	}

	// Upload single resulting object
	err = uploadObject(ctx, s3Client)
	if err != nil {
		return
	}
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
			log.Printf("Obtaining: %v", object.Key)
			obj, err := s3Client.GetObject(context.Background(), sourceBucket /*bucketName*/, object.Key /*objectName*/, minio.GetObjectOptions{})
			if err != nil {
				log.Printf("Failed to obtain object: %v - %v\n", object.Key, err)
				return err
			}
			objectSize += object.Size
			if _, err := io.Copy(buffer, obj); err != nil {
				log.Fatalln(err)
			}
		}
	}
	if objectCount == 0 {
		log.Println("Failed to find objects - exiting")
		return errors.New("no objects found")
	}
	log.Printf("Found objects: %v, size: %v", objectCount, objectSize)

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
	log.Printf("Uploading %s to %s\n", targetObjectName, targetBucketPrefix)
	_, err = s3Client.PutObject(ctx, targetBucket /*bucketName*/, targetObjectName /*objectName*/, buffer /*reader*/, objectSize /*objectSize*/, minio.PutObjectOptions{ContentType: ContentType})
	if err != nil {
		log.Printf("Failed to upload object %v - %v\n", targetObjectName, err)
		return err
	}

	log.Printf("Successfully uploaded %s to %s\n", targetObjectName, targetBucketPrefix)
	return nil
}
