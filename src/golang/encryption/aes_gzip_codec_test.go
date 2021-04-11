package encryption

import (
  "bytes"
  "context"
  "io/ioutil"
  "testing"
  "time"
  //pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

const dummy_pw = "chocolat"
const persisted_key_1 = "vT9bzXu2/8V0/Ufy1159//v9fa773/P/bn/f9+vf338="
const fp_persisted_key_1 = "eXEGy9DCOk0nKu4xwfcIhw=="
const secret_key_1 = "\xb5\x3b\x53\xcd\x7b\x86\xff\xc1\x54\xb4\x44\x92\x07\x52\x59\xcf\x53\xec\x19\x2e\x59\x9f\x70\xb3\x6e\x1d\xdd\x51\x29\xcd\x9f\x3a"
const persisted_key_2 = "7/dL5Fezv1Ss+0vv915s/r8fZf/+1/tvaGfW7+v/2Xc="
const secret_key_2 = "\xe7\xf5\x4b\xe0\x45\x33\xb7\x50\x8c\x72\x49\xaf\x25\x44\x6c\xc8\x95\x1b\x61\x7b\x76\x96\x38\x64\x68\x20\xd0\x89\xab\xa5\xc9\x47"

func buildTestCodec(t *testing.T) *aesGzipCodec {
  keys := []string {persisted_key_1, persisted_key_2,}
  return buildTestCodecChooseEncKey(t, keys)
}

func buildTestCodecChooseEncKey(t *testing.T, keys []string) *aesGzipCodec {
  conf := util.LoadTestConf()
  conf.EncryptionKeys = keys
  fixed_pw := func() (string, error) { return dummy_pw, nil }
  codec, err := NewCodecHelper(conf, fixed_pw)
  if err != nil { t.Fatalf("Could not create codec: %v", err) }
  return codec.(*aesGzipCodec)
}

func TestGenerateEncryptionKey(t *testing.T) {
  codec := buildTestCodec(t)
  secret, persisted := codec.GenerateEncryptionKey()
  t.Logf("secret:%x, persisted:%s", secret.B, persisted.S)
  if len(codec.xor_key.B) < 1 { t.Errorf("Bad xor key") }
  if len(secret.B) < 1 { t.Errorf("Bad secret key") }
  if len(persisted.S) < 1 { t.Errorf("Bad persisted key") }
}

func TestSecretToPersistedKey(t *testing.T) {
  codec := buildTestCodec(t)
  secret, persisted := codec.GenerateEncryptionKey()
  t.Logf("secret:%x, persisted:%s", secret.B, persisted.S)

  dec_persisted := codec.decodeEncryptionKey(persisted)
  if bytes.Compare(secret.B, dec_persisted.B) != 0 {
    t.Errorf("Bad persisted key decoding: %x != %x", secret.B, dec_persisted.B)
  }
  persisted_p := codec.encodeEncryptionKey(dec_persisted)
  if persisted_p.S != persisted.S {
    t.Errorf("Persisted key round trip: %x != %x", persisted_p.S, persisted.S)
  }
  enc_secret := codec.encodeEncryptionKey(secret)
  if enc_secret.S != persisted.S {
    t.Errorf("Bad secret key encoding: %x != %x", enc_secret.S, persisted.S)
  }
  secret_p := codec.decodeEncryptionKey(enc_secret)
  if bytes.Compare(secret.B, secret_p.B) != 0 {
    t.Errorf("Secret key round trip: %x != %x", secret.B, secret_p.B)
  }
}

func TestFingerprintKey(t *testing.T) {
  codec := buildTestCodec(t)
  fp := codec.FingerprintKey(types.PersistableKey{persisted_key_1})
  t.Logf("persisted:%s, fingerprint:%s", persisted_key_1, fp)
  if fp != fp_persisted_key_1 { t.Errorf("Bad fingerprint calculation: %s != %s", fp, fp_persisted_key_1) }
}

func TestEncryptString(t *testing.T) {
  expect_plain := "chocoloco plain text"
  codec := buildTestCodec(t)
  obfus := codec.EncryptString(expect_plain)
  obfus_2 := codec.EncryptString(expect_plain)
  if bytes.Compare(obfus, obfus_2) == 0 {
    t.Errorf("Encrypt of the same string should not produce the same obfuscated bytes")
  }

  plain, err := codec.DecryptString("", obfus)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  t.Logf("obfuscated:%x, plain:%s", obfus, plain)
  if expect_plain != plain {
    t.Errorf("Bad decryption expected:%s, got:%s", expect_plain, plain)
  }
}

func TestEncryptString_Fingerprint(t *testing.T) {
  expect_plain := "chocoloco plain text"
  keys := []string {persisted_key_2, persisted_key_1,}
  codec := buildTestCodec(t)
  codec_2 := buildTestCodecChooseEncKey(t, keys)
  fp := codec.FingerprintKey(types.PersistableKey{persisted_key_2})
  obfus := codec_2.EncryptString(expect_plain)

  plain, err := codec.DecryptString(fp, obfus)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  t.Logf("obfuscated:%x, plain:%s", obfus, plain)
  if expect_plain != plain {
    t.Errorf("Bad decryption expected:%s, got:%s", expect_plain, plain)
  }
}

func TestEncryptStream(t *testing.T) {
  codec := buildTestCodec(t)
  expect_msg := []byte("some plain text data")
  read_pipe := types.NewMockPreloadedPipe(expect_msg).ReadEnd()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  var err error
  var encoded_pipe, decoded_pipe types.PipeReadEnd
  encoded_pipe, err = codec.EncryptStream(ctx, read_pipe)
  if err != nil { t.Fatalf("Could not encrypt: %v", err) }
  decoded_pipe, err = codec.DecryptStream(ctx, "", encoded_pipe)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  done := make(chan []byte)
  go func() {
    defer close(done)
    defer decoded_pipe.Close()
    data, err := ioutil.ReadAll(decoded_pipe)
    if err != nil { t.Errorf("ReadAll: %v", err) }
    if decoded_pipe.GetErr() != nil { t.Errorf("decoded_pipe.GetErr(): %v", decoded_pipe.GetErr()) }
    done <- data
  }()

  select {
    case data := <-done:
      util.CompareAsStrings(t, data, expect_msg)
    case <-ctx.Done():
      t.Fatalf("TestEncryptStream timeout")
  }
}

func TestEncryptStream_MoreData(t *testing.T) {
  codec := buildTestCodec(t)
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  read_pipe := util.ProduceRandomTextIntoPipe(ctx, 4096, 32)

  var err error
  var encoded_pipe, decoded_pipe types.PipeReadEnd
  encoded_pipe, err = codec.EncryptStream(ctx, read_pipe)
  if err != nil { t.Fatalf("Could not encrypt: %v", err) }
  decoded_pipe, err = codec.DecryptStream(ctx, "", encoded_pipe)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  done := make(chan []byte)
  go func() {
    defer close(done)
    defer decoded_pipe.Close()
    data, err := ioutil.ReadAll(decoded_pipe)
    if err != nil { t.Errorf("ReadAll: %v", err) }
    if decoded_pipe.GetErr() != nil { t.Errorf("decoded_pipe.GetErr(): %v", decoded_pipe.GetErr()) }
    done <- data
  }()

  select {
    case data := <-done:
      util.CompareAsStrings(t, len(data), 4096*32)
    case <-ctx.Done():
      t.Fatalf("TestEncryptStream timeout")
  }
}

