package gcsfs

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/gkalele/dfs/dfsapi"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

type Behaviour int

const (
	InvalidBehaviour = iota
	Panic
	Warn
	Ignore
)

type GCS struct {
	BehaviourMode Behaviour
	bucketName    string
}

type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

type FsInfo struct {
	Name string
}

func New(bucketName string, behaviourMode Behaviour) *GCS {
	return &GCS{BehaviourMode: behaviourMode, bucketName: bucketName}
}

type TransactionClient struct {
	txID   uuid.UUID
	client *storage.Client
	bucket *storage.BucketHandle
}

func (g *GCS) generateEphemeralClient(ctx context.Context) (*TransactionClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(g.bucketName)
	if bucket == nil {
		return nil, fmt.Errorf("bucket %s does not exist", g.bucketName)
	}
	return &TransactionClient{
		txID:   uuid.New(),
		client: client,
		bucket: bucket,
	}, nil
}

func (g *GCS) throwUnimplemented(message string) error {
	switch g.BehaviourMode {
	case Panic:
		panic(message)
	case Warn:
		glog.Warning(message)
		return fmt.Errorf(message)
	case Ignore:
		return nil
	}
	panic("Uninitialized gcsfs behaviour mode")
}

func (g *GCS) User(ctx context.Context) string {
	if g.throwUnimplemented("User method not implemented") == nil {
		return ""
	}
	return `User method not implemented`
}

func (g *GCS) ReadFile(ctx context.Context, filename string) ([]byte, error) {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return nil, err
	}
	reader, err := client.bucket.Object(filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func (g *GCS) CopyToLocal(ctx context.Context, src string, dst string) error {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return err
	}
	reader, err := client.bucket.Object(dst).NewReader(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, reader)
	return err
}

func (g *GCS) CopyToRemote(ctx context.Context, src string, dst string) error {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return err
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := client.bucket.Object(dst).NewWriter(ctx)
	defer writer.Close()
	_, err = io.Copy(writer, f)
	return err
}

// Close - there is nothing to do for GCS Close()
func (g *GCS) Close(ctx context.Context) error {
	return nil
}

func (g *GCS) GetContentSummary(ctx context.Context, path string) (*dfsapi.ContentSummary, error) {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return nil, err
	}
	attrs, err := client.bucket.Object(path).Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return &dfsapi.ContentSummary{
		Length:         uint64(attrs.Size),
		FileCount:      0,
		DirectoryCount: 0,
		Quota:          0,
		SpaceConsumed:  0,
		SpaceQuota:     0,
	}, nil
}

func (g *GCS) Open(ctx context.Context, name string) (dfsapi.FileReader, error) {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.bucket.Object(name).NewReader(ctx)
}

func (g *GCS) Create(ctx context.Context, name string) (dfsapi.FileWriter, error) {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.bucket.Object(name).NewWriter(ctx), nil
}

func (g *GCS) CreateFile(ctx context.Context, name string, _ int, _ int64, _ os.FileMode) (dfsapi.FileWriter, error) {
	return g.Create(ctx, name)
}

func (g *GCS) Append(ctx context.Context, name string) (dfsapi.FileWriter, error) {
	err := g.throwUnimplemented("Append method not implemented")
	return nil, err
}

func (g *GCS) CreateEmptyFile(ctx context.Context, name string) error {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return err
	}
	writer := client.bucket.Object(name).NewWriter(ctx)
	return writer.Close()
}

func (g *GCS) Mkdir(ctx context.Context, dirname string, perm os.FileMode) error {
	return nil
}

func (g *GCS) MkdirAll(ctx context.Context, dirname string, perm os.FileMode) error {
	return nil
}

func (g *GCS) Chmod(ctx context.Context, name string, perm os.FileMode) error {
	return g.throwUnimplemented("chmod not implemented")
}

func (g *GCS) Chown(ctx context.Context, name string, user, group string) error {
	return g.throwUnimplemented("chown not implemented")
}

func (g *GCS) Chtimes(ctx context.Context, name string, atime time.Time, mtime time.Time) error {
	return g.throwUnimplemented("chtimes not implemented")
}

func (g *GCS) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return nil, err
	}
	iter := client.bucket.Objects(ctx, &storage.Query{
		Delimiter:                "/",
		Prefix:                   dirname,
		Versions:                 false,
		IncludeTrailingDelimiter: false,
		MatchGlob:                "",
		IncludeFoldersAsPrefixes: false,
		SoftDeleted:              false,
	})
	fileInfos := make([]os.FileInfo, 0, 32)
	for {
		obj, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, err
		}
		fileInfos = append(fileInfos, &FileInfo{
			name:    obj.Name,
			size:    obj.Size,
			mode:    0666,
			modTime: obj.Updated,
			isDir:   false,
			sys:     nil,
		})
	}
	return fileInfos, nil
}

func (g *GCS) Remove(ctx context.Context, name string) error {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return err
	}
	return client.bucket.Object(name).Delete(ctx)
}

// RemoveAll removes path and any children it contains. It removes everything it
// can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func (g *GCS) RemoveAll(ctx context.Context, dirname string) error {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return err
	}
	iter := client.bucket.Objects(ctx, &storage.Query{
		Delimiter:                "",
		Prefix:                   dirname,
		Versions:                 false,
		IncludeTrailingDelimiter: false,
		MatchGlob:                "",
		IncludeFoldersAsPrefixes: false,
		SoftDeleted:              false,
	})
	for {
		obj, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return err
		}
		if err = client.bucket.Object(obj.Name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Rename an existing GCS object
// GCS SDK Documentation states that we must copy the object to the new name and then delete the old one.
//
// > To move or rename an object using the JSON API directly, first make a copy of the object
// > that has the properties you want and then delete the original object.
//
// Found the Copier() API that provides an elegant way to copy large objects.
func (g *GCS) Rename(ctx context.Context, oldpath, newpath string) error {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return err
	}
	// Copier provides a resume token that allows resuming any failed copy operations
	copier := client.bucket.Object(newpath).CopierFrom(client.bucket.Object(oldpath))
	for retries := 0; retries < 5; retries++ {
		if _, err = copier.Run(ctx); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return err
}

func (g *GCS) RenameWithOverwriteOption(ctx context.Context, oldpath, newpath string, overwrite bool) error {
	return g.Rename(ctx, oldpath, newpath)
}

func (g *GCS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	client, err := g.generateEphemeralClient(ctx)
	if err != nil {
		return nil, err
	}
	attrs, err := client.bucket.Object(name).Attrs(ctx)
	return &FileInfo{
		name:    name,
		size:    attrs.Size,
		mode:    0666,
		modTime: attrs.Updated,
		isDir:   false,
		sys:     nil,
	}, nil
}

func (g *GCS) StatFs(_ context.Context) (dfsapi.FsInfo, error) {
	return &FsInfo{
		Name: "gcs",
	}, nil
}

func (g *GCS) Walk(_ context.Context, _ string, _ filepath.WalkFunc) error {
	return g.throwUnimplemented("walk not implemented")
}

func (f *FileInfo) Name() string {
	return f.name
}

func (f *FileInfo) Size() int64 {
	return f.size
}

func (f *FileInfo) Mode() fs.FileMode {
	return f.mode
}

func (f *FileInfo) ModTime() time.Time {
	return f.modTime
}

func (f *FileInfo) IsDir() bool {
	return f.isDir
}

func (f *FileInfo) Sys() any {
	return f.sys
}

func (fs *FsInfo) GetName() string {
	return fs.Name
}
