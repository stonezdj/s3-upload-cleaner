package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/jessevdk/go-flags"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const startedadDateFormat = "2006-01-02T15:04:05Z"

var opts struct {
	Endpoint      string `short:"e" long:"endpoint" description:"The endpoint, for example: https://s3.us-west-1.amazonaws.com" required:"true"`
	Region        string `short:"r" long:"region" description:"Region of the bucket, for example: us-west-1" required:"true"`
	Bucket        string `short:"b" long:"bucket" description:"The bucket name" required:"true"`
	AccessKey     string `short:"a" long:"accesskey" description:"Access Key" required:"true"`
	SecretKey     string `short:"s" long:"secretkey" description:"Secret Key" required:"true"`
	RootDirectory string `short:"d" long:"rootdir" description:"The root directory of the docker registry" default:"" required:"false"`
	CleanupHours  int    `short:"c" long:"cleanup" description:"Cleanup Hours, it will cleanup multipart created before this hours ." default:"3" required:"false"`
	DryRun        bool   `short:"y" long:"dryrun" description:"Dry run" required:"false"`
}

const MaxResultCount = 1000

func main() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	_, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		fmt.Printf("failed to parse command line parameters, error %v", err)
		return
	}
	if opts.DryRun {
		fmt.Println("run cleanup script in DryRun mode")
	}
	totalRemoved := 0
	s := getS3Client(opts.Endpoint, opts.Region, opts.AccessKey, opts.SecretKey)

	fmt.Printf("CleanupHours:%v\n", opts.CleanupHours)
	fmt.Printf("Endpoint: %s\n", *s.Config.Endpoint)
	fmt.Printf("Bucket: %s\n\n", opts.Bucket)
	fmt.Printf("Root Directory is: %s\n\n", opts.RootDirectory)
	prefix := "docker/registry/v2/repositories/"
	if len(opts.RootDirectory) > 0 {
		prefix = opts.RootDirectory + "/docker/registry/v2/repositories/"
	}
	fmt.Printf("prefix is %v", prefix)

	var isTruncated = true
	var marker string
	maxKey := int64(MaxResultCount)
	for isTruncated {
		objs, err := s.ListObjects(&s3.ListObjectsInput{
			MaxKeys:   &maxKey,
			Bucket:    aws.String(opts.Bucket),
			Prefix:    aws.String(prefix),
			Delimiter: aws.String("/"),
			Marker:    &marker,
		})

		if err != nil {
			panic(err)
		}
		isTruncated = *objs.IsTruncated
		if isTruncated {
			fmt.Println("result is truncated, continue to search")
			marker = *objs.NextMarker
		}

		for i, cp := range objs.CommonPrefixes {
			fmt.Printf("Prefix %d: %s\n", i, *cp.Prefix)

			totalRemoved += cleanMPUs(s, opts.Bucket, *cp.Prefix)

		}
		fmt.Printf("Removing upload folders:%v\n", *objs.Prefix)
		if !opts.DryRun {
			cleanUploadFolders(s, opts.Bucket, *objs.Prefix)
		}
	}

	fmt.Printf("  Total MPUs removed: %d\n", totalRemoved)

}

func cleanMPUs(s *s3.S3, bucket, prefix string) (totalRemoved int) {
	totalRemoved = 0
	var isTruncated = true
	var keyMarker string
	for isTruncated {
		resp, err := s.ListMultipartUploads(&s3.ListMultipartUploadsInput{
			Bucket:     aws.String(bucket),
			Prefix:     aws.String(prefix),
			KeyMarker:  &keyMarker,
			MaxUploads: aws.Int64(MaxResultCount),
		})

		if err != nil {
			panic(err)
		}

		isTruncated = *resp.IsTruncated
		keyMarker = *resp.NextKeyMarker
		if isTruncated {
			fmt.Println("output is truncated, continue to search")
		}

		fmt.Printf(" # of MPUs found for prefix: %d\n", len(resp.Uploads))

		for i, multi := range resp.Uploads {
			fmt.Printf("  Upload %d: %s\n", i, *multi.Key)

			hoursSince := int(time.Since(*multi.Initiated).Hours())

			fmt.Printf("  Started %d hours ago\n", hoursSince)

			if hoursSince >= opts.CleanupHours {
				fmt.Printf("bucket %v, key: %+v, uploadID: %+v\n", bucket, *multi.Key, *multi.UploadId)
				if !opts.DryRun {
					_, err = s.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
						Bucket:   aws.String(bucket),
						Key:      multi.Key,
						UploadId: multi.UploadId,
					})
					if err != nil {
						fmt.Printf(" ERROR: %s\n", err)
					} else {
						fmt.Println("   Removed!")
					}
				}
				totalRemoved++
			}
		}
	}
	fmt.Printf("   Removed %v\n", totalRemoved)
	return
}

func cleanUploadFolders(s *s3.S3, bucket, prefix string) {
	shouldContinue := true
	var continuationToken *string
	for shouldContinue {

		objs, err := s.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int64(MaxResultCount),
			ContinuationToken: continuationToken,
		})

		if err != nil {
			fmt.Println(err)
		}

		for _, o := range objs.Contents {
			if strings.Contains(*o.Key, "/_uploads/") && strings.HasSuffix(*o.Key, "/startedat") {
				hoursSince, err := hoursSinceUploadStarted(s, bucket, *o.Key)
				if err != nil {
					fmt.Printf(" ERROR: %s\n", err)
					continue
				}

				if hoursSince >= opts.CleanupHours {
					fmt.Printf("  Removing folder %s (%d hours)\n", *o.Key, hoursSince)
					if !opts.DryRun {
						removeUploadFolder(s, bucket, *o.Key)
					}
				} else {
					fmt.Printf("  Skipping folder %s (%d hours)\n", *o.Key, hoursSince)
				}
			}
		}

		continuationToken = objs.NextContinuationToken
		shouldContinue = *objs.IsTruncated
	}
}

func removeUploadFolder(s *s3.S3, bucket, prefix string) {
	keyParts := strings.Split(prefix, "/")
	uploadsFolder := strings.Join(keyParts[0:len(keyParts)-1], "/")

	objs, err := s.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(uploadsFolder),
	})

	if err != nil {
		panic(err)
	}

	for _, o := range objs.Contents {
		_, err := s.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    o.Key,
		})

		if err != nil {
			panic(err)
		}

		fmt.Printf("    Removing %s\n", *o.Key)
	}

}

func getS3Client(endPoint, region, accessKey, secretAccessKey string) *s3.S3 {
	awsConfig := aws.NewConfig()

	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     accessKey,
				SecretAccessKey: secretAccessKey,
			},
		},
		&credentials.EnvProvider{},
		&credentials.SharedCredentialsProvider{},
		&ec2rolecreds.EC2RoleProvider{Client: ec2metadata.New(session.New())},
	})

	awsConfig.WithS3ForcePathStyle(true)
	awsConfig.WithEndpoint(endPoint)

	awsConfig.WithCredentials(creds)
	awsConfig.WithRegion(region)
	awsConfig.WithDisableSSL(true)

	return s3.New(session.New(awsConfig))
}

func hoursSinceUploadStarted(s *s3.S3, bucket, key string) (int, error) {
	obj, err := s.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return 0, err
	}

	defer obj.Body.Close()
	t, err := parseTimeFromStream(obj.Body)
	if err != nil {
		panic(err)
	}

	return int(time.Since(t).Hours()), nil
}

func parseTimeFromStream(s io.Reader) (time.Time, error) {
	buf := new(bytes.Buffer)

	_, err := buf.ReadFrom(s)
	if err != nil {
		return time.Time{}, err
	}

	dateString := buf.String()
	return time.Parse(startedadDateFormat, dateString)
}
