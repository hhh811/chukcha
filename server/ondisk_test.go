package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestInitLastChunkIdx(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "hc-chunk1"))
	testCreateFile(t, filepath.Join(dir, "hc-chunk10"))
	srv := testNewOndisk(t, dir)

	want := 11
	got := srv.lastChunkIdx

	if got != uint64(want) {
		t.Errorf("Last chunk index = %d, want %d", got, want)
	}
}

func TestGetFileDescriptor(t *testing.T) {
	dir := getTempDir(t)
	testCreateFile(t, filepath.Join(dir, "hc-chunk1"))
	srv := testNewOndisk(t, dir)

	testcases := []struct {
		desc     string
		filename string
		write    bool
		wantErr  bool
	}{
		{
			desc:     "Read from already existing file should not fail",
			filename: "hc-chunk1",
			write:    false,
			wantErr:  false,
		},
		{
			desc:     "Should not overwrite existing files",
			filename: "hc-chunk1",
			write:    true,
			wantErr:  true,
		},
		{
			desc:     "Should not be able to read from files that don't exist",
			filename: "hc-chunk2",
			write:    false,
			wantErr:  true,
		},
		{
			desc:     "Should be able to create files that don't exist",
			filename: "hc-chunk2",
			write:    true,
			wantErr:  false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := srv.getFileDescriptor(tc.filename, tc.write)
			defer srv.forgetFileDescriptor(tc.filename)

			if tc.wantErr && err == nil {
				t.Errorf("wanted err, got no errors")
			} else if !tc.wantErr && err != nil {
				t.Errorf("wanted no errors, get err %v", err)
			}
		})
	}
}

func TestReadWrite(t *testing.T) {
	srv := testNewOndisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"

	if err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := 1, len(chunks); want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	chunk := chunks[0].Name

	var b bytes.Buffer
	if err = srv.Read(chunk, 0, uint64(len(want)), &b); err != nil {
		t.Fatalf("Read(%q, 0, %d) = %v, want want no errors", chunk, uint64(len(want)), err)
	}

	got := b.String()
	if got != want {
		t.Errorf("Read(%q) = %q, want %q", chunk, got, want)
	}

	// Check that the last message is not chopped only the first
	// three messages are returned if the buffer size is too small to
	// fit all messages
	want = "one\ntwo\nthree\n"
	b.Reset()
	if err = srv.Read(chunk, 0, uint64(len(want)+1), &b); err != nil {
		t.Fatalf("Read(%q, 0, %d) = %v, want want no errors", chunk, uint64(len(want)+1), err)
	}

	got = b.String()
	if got != want {
		t.Errorf("Read(%q) = %q, want %q", chunk, got, want)
	}
}

func TestAckOfTheLastChunk(t *testing.T) {
	srv := testNewOndisk(t, getTempDir(t))

	want := "one\ntwo\nthree\nfour\n"

	if err := srv.Write(context.Background(), []byte(want)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	chunks, err := srv.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks(): %v", err)
	}

	if got, want := 1, len(chunks); want != got {
		t.Fatalf("len(ListChunks()) = %d, want %d", got, want)
	}

	if err := srv.Ack(chunks[0].Name, chunks[0].Size); err == nil {
		t.Errorf("Ack(last chunk): got no errors, expected an error")
	}
}

func TestAckOfTheCompleteChunk(t *testing.T) {
	dir := getTempDir(t)
	srv := testNewOndisk(t, dir)
	testCreateFile(t, filepath.Join(dir, "hc-chunk1"))

	if err := srv.Ack("hc-chunk1", 0); err != nil {
		t.Errorf("Ack(hc-chunk1) = %v, expected no errors", err)
	}
}

func getTempDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp(os.TempDir(), "lastcunkIdx")
	if err != nil {
		t.Fatalf("mkdir temp failed: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	return dir
}

type nilHooks struct{}

func (n *nilHooks) BeforeCreatingChunk(ctx context.Context, category string, fileName string) error {
	return nil
}

func testNewOndisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOndisk(dir, "test", "hc", &nilHooks{})
	if err != nil {
		t.Fatalf("NewOndisk(): %v", err)
	}

	return srv
}

func testCreateFile(t *testing.T, filename string) {
	t.Helper()

	if _, err := os.Create(filename); err != nil {
		t.Fatalf("could not create file %q: %v", filename, err)
	}
}
