package encryption

import (
  "bytes"
  "compress/gzip"
  "io"
  "testing"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

func TestAesEncryptDecryptString(t *testing.T) {
  key, _ := TestOnlyFixedPw()
  expect_plain := types.SecretString{"chocoloco plain text"}
  obfus := AesEncryptString(key, expect_plain)
  obfus_2 := AesEncryptString(key, expect_plain)
  if obfus.S  == obfus_2.S {
    t.Errorf("Encrypt of the same string should not produce the same obfuscated bytes")
  }

  plain, err := AesDecryptString(key, obfus)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  t.Logf("obfuscated:%x, plain:%s", obfus, plain)
  util.EqualsOrFailTest(t, "Bad decryption", plain, expect_plain)
}

func TestCompression(t *testing.T) {
  msg := []byte("original message to compress")
  src_buf := bytes.NewBuffer(msg)

  comp_buf := new(bytes.Buffer)
  comp_writer := gzip.NewWriter(comp_buf)
  io.Copy(comp_writer, src_buf)
  comp_writer.Close()

  decomp_buf := new(bytes.Buffer)
  decomp_reader,err := gzip.NewReader(comp_buf)
  if err != nil { t.Fatalf("gzip.NewReader: %v", err) }
  io.Copy(decomp_buf, decomp_reader)

  util.EqualsOrFailTest(t, "Bad decompression", msg, decomp_buf.Bytes())
}

