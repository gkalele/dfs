// ioutils for DFS

// An implementation of the dfs/ioutil interface for Google Cloud Storage (GCS)
package gcsutil

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/gkalele/dfs/ioutil"
)

type GCSUtil struct {
	client     *storage.Client
	bucketName string
	bucket     *storage.BucketHandle
}

func New(ctx context.Context, bucket string) (*GCSUtil, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &GCSUtil{
		client:     client,
		bucketName: bucket,
		bucket:     client.Bucket(bucket),
	}, nil
}

func (g *GCSUtil) StreamIntoDFS(ctx context.Context, reader io.Reader, name string, overwrite bool) (int64, error) {
	ctxReader := ioutil.NewContextAwareReader(ctx, reader)
	o := g.bucket.Object(name)
	if !overwrite {
		o = o.If(storage.Conditions{DoesNotExist: true})
	}
	wc := o.NewWriter(ctx)
	n, err := io.Copy(wc, ctxReader)
	if err != nil {
		return n, fmt.Errorf("Error copying from input stream to gcs://%s :  %s", filepath.Join(g.bucketName, name), err)
	}
	if err = wc.Close(); err != nil {
		return n, err
	}
	return n, nil
}

func (g *GCSUtil) StreamFromDFS(ctx context.Context, client *storage.Client, bucketName, object string, writer io.WriteCloser) (int64, error) {
	rc, err := client.Bucket(bucketName).Object(object).NewReader(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to read object gcs://%s: %s", filepath.Join(bucketName, object), err.Error())
	}
	defer rc.Close()

	n, err := io.Copy(writer, rc)
	if err != nil {
		return n, fmt.Errorf("failed to fully copy file from GCS - %s", err.Error())
	}

	if err = writer.Close(); err != nil {
		return n, fmt.Errorf("failed to close file gcs://%s: %s", filepath.Join(bucketName, object), err.Error())
	}
	return n, nil
}
