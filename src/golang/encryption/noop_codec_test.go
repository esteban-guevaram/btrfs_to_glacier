package encryption

import (
  "context"
  "io"
  "testing"
  "time"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
)

func TestNoopDecryptString(t *testing.T) {
  expect_plain := types.PersistableString{"chocoloco plain text"}
  codec := new(NoopCodec)

  plain, err := codec.DecryptString(types.PersistableString{""}, expect_plain)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }
  util.EqualsOrFailTest(t, "Bad decryption", plain, expect_plain)
}

func TestNoopEncryptString(t *testing.T) {
  expect_plain := types.SecretString{"chocoloco plain text"}
  codec := new(NoopCodec)

  plain := codec.EncryptString(expect_plain)
  util.EqualsOrFailTest(t, "Bad encryption", plain, expect_plain)
}

func TestNoopDecryptStream(t *testing.T) {
  codec := new(NoopCodec)
  expect_plain := []byte("salutcoucou")
  read_pipe := mocks.NewPreloadedPipe(expect_plain).ReadEnd()
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  decoded_pipe, err := codec.DecryptStream(ctx, types.PersistableString{""}, read_pipe)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer decoded_pipe.Close()
    plain, err := io.ReadAll(decoded_pipe)
    if err != nil { t.Errorf("ReadAll: %v", err) }
    util.EqualsOrFailTest(t, "Bad encryption", plain, expect_plain)
  }()
  util.WaitForClosure(t, ctx, done)
}

func TestNoopDecryptStreamLeaveSinkOpen(t *testing.T) {
  codec := new(NoopCodec)
  expect_plain := []byte("salutcoucou")
  read_pipe := mocks.NewPreloadedPipe(expect_plain).ReadEnd()
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  collect_pipe := mocks.NewMockCollectPipe(ctx)

  err := codec.DecryptStreamLeaveSinkOpen(
    ctx, types.PersistableString{""}, read_pipe, collect_pipe.WriteEnd())
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  select {
    case <-collect_pipe.Done:
      t.Fatalf("WriteEnd should not have been closed")
    case <-time.After(util.SmallTimeout):
      collect_pipe.WriteEnd().Close()
  }
  plain := collect_pipe.BlockingResult(ctx)
  util.EqualsOrFailTest(t, "Bad decryption", plain, expect_plain)
}

func TestNoopEncryptStream(t *testing.T) {
  codec := new(NoopCodec)
  expect_plain := []byte("salutcoucou")
  read_pipe := mocks.NewPreloadedPipe(expect_plain).ReadEnd()
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  encoded_pipe, err := codec.EncryptStream(ctx, read_pipe)
  if err != nil { t.Fatalf("Could not encrypt: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer encoded_pipe.Close()
    plain, err := io.ReadAll(encoded_pipe)
    if err != nil { t.Errorf("ReadAll: %v", err) }
    util.EqualsOrFailTest(t, "Bad encryption", plain, expect_plain)
  }()
  util.WaitForClosure(t, ctx, done)
}

