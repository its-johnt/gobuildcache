package main

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcp"
)

// fakeGCS stands in for the Cloud Storage API. It answers the first failures
// requests with a 503 and the rest with success, and each request takes delay.
type fakeGCS struct {
	failures int64
	delay    time.Duration

	requests atomic.Int64
	canceled atomic.Int64
}

func (f *fakeGCS) RoundTrip(r *http.Request) (*http.Response, error) {
	n := f.requests.Add(1)
	if r.Body != nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}

	select {
	case <-time.After(f.delay):
	case <-r.Context().Done():
		f.canceled.Add(1)
		return nil, r.Context().Err()
	}

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(`{"bucket":"bucket","name":"key"}`)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    r,
	}
	if n <= f.failures {
		resp.StatusCode = http.StatusServiceUnavailable
		resp.Status = "503 Service Unavailable"
		resp.Body = http.NoBody
	}
	return resp, nil
}

// alwaysUnavailable returns a fakeGCS that answers every request with a 503.
func alwaysUnavailable() *fakeGCS { return &fakeGCS{failures: math.MaxInt64} }

// gcsBucket returns a bucket that uses the gcsblob driver and sends its
// requests to transport.
func gcsBucket(t *testing.T, transport http.RoundTripper) *blob.Bucket {
	t.Helper()

	bucket, err := gcsblob.OpenBucket(context.Background(),
		gcp.NewAnonymousHTTPClient(transport), "bucket", nil)
	if err != nil {
		t.Fatalf("opening gcs bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

	return bucket
}

// cacheBucket returns a Bucket with an empty disk cache in front of bucket.
func cacheBucket(t *testing.T, bucket *blob.Bucket) *Bucket {
	t.Helper()

	diskDir := t.TempDir()
	for _, sub := range []string{actionDir, outputDir} {
		if err := os.Mkdir(filepath.Join(diskDir, sub), 0o700); err != nil {
			t.Fatalf("creating %s: %v", sub, err)
		}
	}

	return &Bucket{disk: &Disk{cacheDir: diskDir}, bucket: bucket}
}

func shortenUploadTimeout(t *testing.T) {
	orig := uploadTimeout
	uploadTimeout = 500 * time.Millisecond
	t.Cleanup(func() { uploadTimeout = orig })
}

// Any backend other than GCS leaves As unsatisfied, and using the client
// regardless would dereference a nil pointer.
func TestSetUploadRetryLeavesOtherBackendsAlone(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	setUploadRetry(bucket)
}

func TestUploadRetry(t *testing.T) {
	upload := func(t *testing.T, retry bool) (*fakeGCS, error) {
		t.Helper()

		transport := &fakeGCS{failures: 1}
		bucket := gcsBucket(t, transport)
		if retry {
			setUploadRetry(bucket)
		}

		ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
		defer cancel()
		err := bucket.Upload(ctx, "key", bytes.NewReader(nil),
			&blob.WriterOptions{ContentType: "text/plain"})
		return transport, err
	}

	t.Run("without the policy one 503 fails the upload", func(t *testing.T) {
		transport, err := upload(t, false)
		if err == nil {
			t.Error("upload returned nil after a 503")
		}
		if got := transport.requests.Load(); got != 1 {
			t.Errorf("made %d requests, want 1", got)
		}
	})

	t.Run("with the policy the upload recovers", func(t *testing.T) {
		transport, err := upload(t, true)
		if err != nil {
			t.Errorf("upload returned %v, want nil", err)
		}
		if got := transport.requests.Load(); got != 2 {
			t.Errorf("made %d requests, want 2", got)
		}
	})
}

func TestLinkActionToOutputGivesUp(t *testing.T) {
	shortenUploadTimeout(t)

	blobBucket := gcsBucket(t, alwaysUnavailable())
	setUploadRetry(blobBucket)
	bucket := cacheBucket(t, blobBucket)

	// Deliberately an unbounded context: the deadline under test is the one
	// LinkActionToOutput applies for itself.
	done := make(chan error, 1)
	go func() {
		_, err := bucket.LinkActionToOutput(context.Background(), "abc", "def")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("LinkActionToOutput returned nil against a 503")
		}
	case <-time.After(10 * uploadTimeout):
		t.Fatal("LinkActionToOutput did not give up; the upload deadline is missing")
	}
}

// uploadOutput sends one output of size bytes through the Start workers and
// waits for the upload to end.
func uploadOutput(t *testing.T, transport http.RoundTripper, size int) {
	t.Helper()

	blobBucket := gcsBucket(t, transport)
	setUploadRetry(blobBucket)
	bucket := cacheBucket(t, blobBucket)
	bucket.Start(context.Background())

	if _, _, err := bucket.PutOutput(context.Background(), "output",
		bytes.NewReader(make([]byte, size))); err != nil {
		t.Fatalf("PutOutput returned %v", err)
	}

	done := make(chan struct{})
	go func() {
		bucket.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * outputUploadTimeout(int64(size))):
		t.Fatal("the output upload did not give up; its deadline is missing")
	}
}

func TestOutputUploadDeadline(t *testing.T) {
	shortenUploadTimeout(t)

	t.Run("gives up against a 503", func(t *testing.T) {
		uploadOutput(t, alwaysUnavailable(), 0)
	})

	// A healthy upload that takes longer than uploadTimeout must finish when
	// the output is large enough to account for it.
	t.Run("lets a slow large upload finish", func(t *testing.T) {
		transport := &fakeGCS{delay: 2 * uploadTimeout}
		uploadOutput(t, transport, 2*minUploadRate)
		if got := transport.canceled.Load(); got != 0 {
			t.Errorf("the deadline canceled %d requests, want 0", got)
		}
		if got := transport.requests.Load(); got != 1 {
			t.Errorf("made %d requests, want 1", got)
		}
	})
}
