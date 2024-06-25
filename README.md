<img src="docs/gopher.png" alt="gopher" align="right" width="200"/>

A Unified Distributed Filesystem for Go
===========

There are many FileSystem interfaces for Go, this is another one.
This one is specifically written to allow Java applications that customarily write to HDFS to be able to write to Distributed File Systems such as GCS, S3 etc with the same semantics.

The main FileSystem interface is therefore primary borrowed from the Hadoop FileSystem API.

```
type FileSystem interface {
	// User returns the user that the ClientImpl is acting under. This is either the
	// current system user or the kerberos principal.
	User(ctx context.Context) string
	// ReadFile reads the file named by filename and returns the contents.
	ReadFile(ctx context.Context, filename string) ([]byte, error)
	// CopyToLocal copies the HDFS file specified by src to the local file at dst.
	// If dst already exists, it will be overwritten.
	CopyToLocal(ctx context.Context, src string, dst string) error
	// CopyToRemote copies the local file specified by src to the HDFS file at dst.
	CopyToRemote(ctx context.Context, src string, dst string) error
	// Close terminates all underlying socket connections to remote server.
	Close(ctx context.Context) error
	// GetContentSummary returns a ContentSummary representing the named file or
	// directory. The summary contains information about the entire tree rooted
	// in the named file; for instance, it can return the total size of all
	GetContentSummary(ctx context.Context, name string) (*ContentSummary, error)
	// Open returns an FileReaderImpl which can be used for reading.
	Open(ctx context.Context, name string) (FileReader, error)
	// Create opens a new file in HDFS with the default replication, block size,
	// and permissions (0644), and returns an io.WriteCloser for writing
	// to it. Because of the way that HDFS writes are buffered and acknowledged
	// asynchronously, it is very important that Close is called after all data has
	// been written.
	Create(ctx context.Context, name string) (FileWriter, error)
	// CreateFile opens a new file in HDFS with the given replication, block size,
	// and permissions, and returns an io.WriteCloser for writing to it. Because of
	// the way that HDFS writes are buffered and acknowledged asynchronously, it is
	// very important that Close is called after all data has been written.
	CreateFile(ctx context.Context, name string, replication int, blockSize int64, perm os.FileMode) (FileWriter, error)
	// Append opens an existing file in HDFS and returns an io.WriteCloser for
	// writing to it. Because of the way that HDFS writes are buffered and
	// acknowledged asynchronously, it is very important that Close is called after
	// all data has been written.
	Append(ctx context.Context, name string) (FileWriter, error)
	// CreateEmptyFile creates a empty file at the given name, with the
	// permissions 0644.
	CreateEmptyFile(ctx context.Context, name string) error
	// Mkdir creates a new directory with the specified name and permission bits.
	Mkdir(ctx context.Context, dirname string, perm os.FileMode) error
	// MkdirAll creates a directory for dirname, along with any necessary parents,
	// and returns nil, or else returns an error. The permission bits perm are used
	// for all directories that MkdirAll creates. If dirname is already a directory,
	// MkdirAll does nothing and returns nil.
	MkdirAll(ctx context.Context, dirname string, perm os.FileMode) error
	// Chmod changes the mode of the named file to mode.
	Chmod(ctx context.Context, name string, perm os.FileMode) error
	// Chown changes the user and group of the file. Unlike os.Chown, this takes
	// a string username and group (since that's what HDFS uses.)
	//
	// If an empty string is passed for user or group, that field will not be
	// changed remotely.
	Chown(ctx context.Context, name string, user, group string) error

	// Chtimes changes the access and modification times of the named file.
	Chtimes(ctx context.Context, name string, atime time.Time, mtime time.Time) error
	// ReadDir reads the directory named by dirname and returns a list of sorted
	// directory entries.
	//
	// The os.FileInfo values returned will not have block location attached to
	// the struct returned by Sys().
	ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error)
	// Remove removes the named file or (empty) directory.
	Remove(ctx context.Context, name string) error
	// RemoveAll removes path and any children it contains. It removes everything it
	// can but returns the first error it encounters. If the path does not exist,
	// RemoveAll returns nil (no error).
	RemoveAll(ctx context.Context, name string) error
	// Rename renames (moves) a file.
	Rename(ctx context.Context, oldpath, newpath string) error
	// RenameWithOverwrite renames (moves) a file. Overwrite option is taken as input.
	RenameWithOverwriteOption(ctx context.Context, oldpath, newpath string, overwrite bool) error
	// Stat returns an os.FileInfo describing the named file or directory.
	Stat(ctx context.Context, name string) (os.FileInfo, error)
	StatFs(ctx context.Context) (FsInfo, error)
	// Walk walks the file tree rooted at root, calling walkFn for each file or
	// directory in the tree, including root.All errors that arise visiting files
	// and directories are filtered by walkFn. The files are walked in lexical
	// order, which makes the output deterministic but means that for very large
	// directories Walk can be inefficient. Walk does not follow symbolic links.
	Walk(ctx context.Context, root string, walkFn filepath.WalkFunc) error
}
```


Compatibility Modes
-------------------

Instead of supporting all operations, we only support operations that make sense on immutable blob stores.
For unimplemented operations, we support 3 modes

- ignore and return no errors
- return a 'Not Implemented' error for graceful error handling, this has the drawback that if error handling is weak or non-existent, implementors using this library might fail silently.
- panic and die, hopefully all such unimplemented calls will thus be exposed and caught during testing.
