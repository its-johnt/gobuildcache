package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

const (
	actionDir = "action"
	outputDir = "output"
)

type OutputInfo struct {
	ID   string
	Path string
	Size int64
	Time int64
}

type Storage interface {
	PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error)
	GetOutput(ctx context.Context, outputID string) (string, error)

	OutputIDFromAction(ctx context.Context, actionID string) (string, error)
	LinkActionToOutput(ctx context.Context, actionID, outputID string) error
}

type Disk struct {
	cacheDir string
}

type Bucket struct {
	disk   *Disk
	bucket *blob.Bucket
	jobs   chan string
	wg     sync.WaitGroup
}

func (d *Disk) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	outputPathname := filepath.Join(d.cacheDir, outputDir, outputID)

	// do nothing if already exists
	if _, err := os.Stat(outputPathname); err == nil {
		return outputPathname, true, nil
	}

	slog.Debug("persisting to disk", "path", outputPathname)

	f, err := os.CreateTemp(d.cacheDir, "output")
	if err != nil {
		return "", false, fmt.Errorf("creating temporary output file: %w", err)
	}
	defer os.RemoveAll(f.Name())
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		return "", false, fmt.Errorf("copying output to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", false, fmt.Errorf("flushing output to disk: %w", err)
	}

	if err := os.Rename(f.Name(), outputPathname); err != nil {
		return "", false, fmt.Errorf("renaming: %w", err)
	}

	return outputPathname, false, nil
}

func (d *Disk) GetOutput(ctx context.Context, outputID string) (string, error) {
	return filepath.Join(d.cacheDir, outputDir, outputID), nil
}

func (d *Disk) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)

	outputPathname, err := os.Readlink(actionPathname)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return filepath.Base(outputPathname), nil
}

func (d *Disk) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	actionPathname := filepath.Join(d.cacheDir, actionDir, actionID)
	outputPathname := filepath.Join("..", outputDir, outputID)

	// Check if existing symlink already points to the correct output
	existing, err := os.Readlink(actionPathname)
	if err == nil && existing == outputPathname {
		return true, nil
	}

	// Atomically create/replace symlink by creating at temp path then renaming
	tmpPathname := fmt.Sprintf("%s.tmp.%x", actionPathname, rand.Uint64())
	// Conceivably the temporary filename could already exist and this would
	// error, but it seems unlikely enough to not worry about.
	if err := os.Symlink(outputPathname, tmpPathname); err != nil {
		return false, err
	}
	if err := os.Rename(tmpPathname, actionPathname); err != nil {
		os.Remove(tmpPathname)
		return false, err
	}
	return false, nil
}

func (b *Bucket) OutputIDFromAction(ctx context.Context, actionID string) (string, error) {
	outputID, err := b.disk.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", fmt.Errorf("output id from action (disk): %w", err)
	}

	if outputID != "" {
		slog.Debug("returning output id", "action", actionID, "output", outputID)
		return outputID, nil
	}

	// TODO: come up with a better solution for this scenario
	// If we fetch from remote storage and there's nothing there, we store an "empty" link,
	// just so that we don't keep trying to fetch this (it adds latency, only to find nothing).
	// The downside is that if at some point it does exist in remote storage, we might not
	// immediately observe that.
	cacheEmptyOutputPath := filepath.Join(b.disk.cacheDir, actionDir, actionID+".empty")
	if _, err := os.Stat(cacheEmptyOutputPath); err == nil {
		slog.Debug("empty found", "action", actionID, "output", outputID)
		return "", nil
	}

	attr, err := b.bucket.Attributes(ctx, path.Join(actionDir, actionID))
	slog.Debug("fetched attributes", "action", actionID, "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		slog.Debug("created found", "action", actionID, "output", outputID)
		os.WriteFile(cacheEmptyOutputPath, nil, 0o600)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("attribute for %v: %w", actionID, err)
	}

	outputID = attr.Metadata["output_id"]
	if outputID == "" {
		slog.Debug("no metadata output id", "action", actionID, "output", outputID)
		return "", nil
	}

	slog.Debug("linking action to output from output from action", "action", actionID, "output", outputID)
	b.disk.LinkActionToOutput(ctx, actionID, outputID)

	return outputID, nil
}

// uploadTimeout bounds one upload of a small object, retries included. It is
// also the base of an output's deadline; see outputUploadTimeout.
//
// A deadline is needed because the retry loop in the API client runs "up to
// the context deadline", and storage.WithMaxAttempts does not reach it.
// Without one, an outage holds the go command open, because the retry never
// ends.
//
// It is a var only so that tests can shrink it; nothing else writes it.
var uploadTimeout = 10 * time.Second

// minUploadRate is the slowest rate, in bytes per second, at which an output
// upload may transfer before its deadline stops it. Up to 20 uploads run at
// once, so the link must carry 20 times this rate for all of them to finish.
const minUploadRate = 1 << 20

// outputUploadTimeout gives an output of size bytes the time to transfer at
// minUploadRate on top of uploadTimeout. The deadline covers the transfer as
// well as the retries, so a fixed one would stop a healthy upload of a large
// output.
func outputUploadTimeout(size int64) time.Duration {
	return uploadTimeout + time.Duration(size/minUploadRate)*time.Second
}

// setUploadRetry makes the GCS client retry object writes.
//
// Reads are idempotent, so the client already retries those. An unconditional
// write is not, so under the default RetryIdempotent policy a single transient
// error, such as a 503, fails the upload. For an action marker that fails the
// go command; for an output it loses the object.
//
// RetryAlways is safe for what this uploads. Outputs are named by their
// content, and a retry of an action marker resends the same empty body and
// metadata, so writing either one twice is the same as writing it once.
//
// Only GCS needs this. The AWS and Azure SDKs retry on their own, and a bucket
// backed by anything else leaves As unsatisfied and is left alone.
func setUploadRetry(bucket *blob.Bucket) {
	var client *storage.Client
	if !bucket.As(&client) {
		return
	}

	client.SetRetry(storage.WithPolicy(storage.RetryAlways))
}

func (b *Bucket) LinkActionToOutput(ctx context.Context, actionID, outputID string) (bool, error) {
	exists, err := b.disk.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil || exists {
		return exists, err
	}

	ctx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()

	return false, b.bucket.Upload(ctx, path.Join(actionDir, actionID), bytes.NewReader(nil), &blob.WriterOptions{
		Metadata:    map[string]string{"output_id": outputID},
		ContentType: "plain/text",
	})
}

func (b *Bucket) PutOutput(ctx context.Context, outputID string, r io.Reader) (string, bool, error) {
	pathname, exists, err := b.disk.PutOutput(ctx, outputID, r)
	if err != nil {
		return "", false, err
	}
	if exists {
		return pathname, true, nil
	}

	slog.Debug("scheduling upload", "path", pathname)
	b.jobs <- pathname

	return pathname, false, nil
}

func (b *Bucket) Start(ctx context.Context) {
	// queue up to 1000
	b.jobs = make(chan string, 1000)

	// 20 workers ought to be enough for anybody
	for i := 0; i < 20; i++ {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			for pathname := range b.jobs {
				f, err := os.Open(pathname)
				if err != nil {
					slog.Error("opening file for upload", "path", pathname, "err", err)
					continue
				}

				info, err := f.Stat()
				if err != nil {
					f.Close()
					slog.Error("reading file size for upload", "path", pathname, "err", err)
					continue
				}

				now := time.Now()
				uploadCtx, cancel := context.WithTimeout(ctx, outputUploadTimeout(info.Size()))
				err = b.bucket.Upload(uploadCtx, path.Join(outputDir, filepath.Base(pathname)), f, &blob.WriterOptions{ContentType: "application/octet-stream"})
				cancel()
				f.Close()
				if err != nil {
					slog.Error("uploading file", "path", pathname, "err", err, "took", time.Since(now))
				} else {
					slog.Debug("uploaded file", "path", pathname, "took", time.Since(now))
				}
			}
		}()
	}
}

func (b *Bucket) Close() {
	slog.Debug("waiting for uploads...")

	now := time.Now()
	close(b.jobs)
	b.wg.Wait()

	slog.Debug("waited for uploads", "took", time.Since(now))
}

func (b *Bucket) GetOutput(ctx context.Context, outputID string) (string, error) {
	slog.Debug("getting output from disk", "output", outputID)

	pathname, err := b.disk.GetOutput(ctx, outputID)
	if err != nil {
		return "", err
	}

	slog.Debug("got output from disk", "output", outputID, "path", pathname, "err", err)

	if _, err := os.Stat(pathname); err == nil {
		slog.Debug("returning pathname", "output", outputID, "path", pathname)

		return pathname, nil
	}

	slog.Debug("downloading", "output", outputID)

	buf := new(bytes.Buffer)
	err = b.bucket.Download(ctx, path.Join(outputDir, outputID), buf, &blob.ReaderOptions{})
	slog.Debug("downloaded", "output", outputID, "err", err)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	slog.Debug("putting download to disk", "output", outputID, "size", buf.Len())

	pathname, _, err = b.disk.PutOutput(ctx, outputID, bytes.NewReader(buf.Bytes()))

	slog.Debug("putting download to disk done", "output", outputID, "size", buf.Len())

	return pathname, err
}
