package integration

import (
	"bytes"
	"chukcha/client"
	"context"
	"fmt"
	"testing"
	"time"
)

func ensureChunkDoesNotExist(t *testing.T, cl *client.Simple, addr string, chunk string, errMsg string) {
	chunks, err := cl.ListChunks(context.Background(), "race", addr, false)
	if err != nil {
		t.Fatalf("ListChunks() at %q returned an error: %v", addr, err)
	}

	for _, ch := range chunks {
		if ch.Name == chunk {
			t.Fatalf("Unexpected chunk %q at %q, wanted it to be absent (%s)", chunk, addr, errMsg)
		}
	}
}

func ensureChunkExists(t *testing.T, cl *client.Simple, addr string, chunk string, errMsg string) {
	chunks, err := cl.ListChunks(context.Background(), "race", addr, false)
	if err != nil {
		t.Fatalf("ListChunks() at %q returned an error: %v", addr, err)
	}

	for _, ch := range chunks {
		if ch.Name == chunk {
			return
		}
	}

	t.Fatalf("Chunk %q not found at %q, wanted it to be present", chunk, addr)
}

func waitUntilChunkAppears(t *testing.T, cl *client.Simple, addr string, chunk string, errMsg string) {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timed out waiting for chunk %q to apper at %q (%s)", chunk, addr, errMsg)
		default:
		}

		chunks, err := cl.ListChunks(ctx, "race", addr, false)
		if err != nil {
			t.Fatalf("ListChunks() at %q returned an error: %v", addr, err)
		}

		for _, ch := range chunks {
			if ch.Name == chunk {
				return
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func hc1ChunkName(idx int) string {
	return fmt.Sprintf("hc1-chunk%09d", idx)
}

func mustSend(t *testing.T, cl *client.Simple, ctx context.Context, category string, msgs []byte) {
	err := cl.Send(ctx, category, msgs)
	if err != nil {
		t.Fatalf("Failed to send the following messages: %q with error: %v", string(msgs), err)
	}
}

func TestReplicatingAlreadyAcknowledgedChunk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	addrs := runChukcha(t, true, tweaks{
		modifyInitArgs: func(t *testing.T, a *InitArgs) {
			a.DisableAcknowledge = true
			a.MaxChunkSize = 10
		},
	})

	client1 := client.NewSimple(addrs[0:1])
	client2 := client.NewSimple(addrs[1:2])

	firstMsg := "h1 is having chunk0 which starts to replicate to h2 immediately\n"

	mustSend(t, client1, ctx, "race", []byte(firstMsg))
	ensureChunkExists(t, client1, addrs[0], hc1ChunkName(0), "Chunk0 must be present after first send")

	mustSend(t, client1, ctx, "race", []byte("h1 now has chunk1 that is being written into currently\n"))
	ensureChunkExists(t, client1, addrs[0], hc1ChunkName(1), "Chunk1 must be present after second send")

	waitUntilChunkAppears(t, client2, addrs[1], hc1ChunkName(1), "h2 must have chunk1 via replication")

	err := client2.Process(ctx, "race", nil, func(msg []byte) error {
		if !bytes.Equal(msg, []byte(firstMsg)) {
			t.Fatalf("Read unexpected message: %q instead of %q", string(msg), firstMsg)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("No errors expected during processing of the first chunk in h2: %v", err)
	}

	ensureChunkExists(t, client2, addrs[1], hc1ChunkName(0), "CHunk0 must not have been acknowledged when reading from h2 the first time")

	err = client2.Process(ctx, "race", nil, func(b []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("No errors expected during processing of the second chunk in h2: %v", err)
	}

	ensureChunkDoesNotExist(t, client2, addrs[1], hc1ChunkName(0), "Chunk0 must have been acknowledged when reading from h2")

	mustSend(t, client1, ctx, "race", []byte("h1 now has chunk2 that is being written int currently, and this will also create an entry to replication which will try to download chunk0 on h2 once again, even though it was acknowledged on h2, and as acknowledge thread is disabled in this test, it will mean that h2 will download chunk0 once again\n"))
	ensureChunkExists(t, client1, addrs[0], hc1ChunkName(2), "Chunk2 must be present after third send()")

	waitUntilChunkAppears(t, client2, addrs[1], hc1ChunkName(2), "Chunk2 must be on h2 via replication")

	ensureChunkDoesNotExist(t, client2, addrs[1], hc1ChunkName(0), "Chunk0 must not be present on h2 because it was previously adknowledged")
}
