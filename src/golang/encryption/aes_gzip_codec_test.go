package encryption

import (
  "bytes"
  "compress/gzip"
  "context"
  "io"
  "io/ioutil"
  "testing"
  "time"
  //pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

// xor_key=`sha256sum <(echo -n "dummy_pw") | cut -f1 -d' ' | sed -r 's/(..)/\\\\x\1/g'`
const dummy_pw = "chocolat"
// persisted=`python3 -c "print(b''.join((b'$secret'[i] ^ b'$xor_key'[i]).to_bytes(1,'big') for i in range(32)).hex(), end='')" | xxd -r -p | base64`
const persisted_key_1 = "OC0aSSg2woV0bUfw0Ew1+ej5fYCzzIPcTnqbtuKXzk8="
// fp=`sha512sum <(printf 'secret') | head -c 32 | xxd -r -p | base64`
const fp_persisted_key_1 = "vcshfZbP1EYCcGE66Cznmg=="
const secret_key_1 = "\xb5\x3b\x53\xcd\x7b\x86\xff\xc1\x54\xb4\x44\x92\x07\x52\x59\xcf\x53\xec\x19\x2e\x59\x9f\x70\xb3\x6e\x1d\xdd\x51\x29\xcd\x9f\x3a"
const persisted_key_2 = "auMCZBaDihSsq0rN8loA/i4OBdWcxcsLSEeWbmD/mDI="
const secret_key_2 = "\xe7\xf5\x4b\xe0\x45\x33\xb7\x50\x8c\x72\x49\xaf\x25\x44\x6c\xc8\x95\x1b\x61\x7b\x76\x96\x38\x64\x68\x20\xd0\x89\xab\xa5\xc9\x47"
const fp_persisted_key_2 = "RWgSa2EIDmUr5FFExM7AgQ=="

func buildTestCodec(t *testing.T) *aesGzipCodec {
  keys := []string {persisted_key_1, persisted_key_2,}
  return buildTestCodecChooseEncKey(t, keys)
}

func buildTestCodecChooseEncKey(t *testing.T, keys []string) *aesGzipCodec {
  conf := util.LoadTestConf()
  conf.EncryptionKeys = keys
  fixed_pw := func() ([]byte, error) { return []byte(dummy_pw), nil }
  codec, err := NewCodecHelper(conf, fixed_pw)
  if err != nil { t.Fatalf("Could not create codec: %v", err) }
  return codec.(*aesGzipCodec)
}

func TestKeyCodec(t *testing.T) {
  codec := buildTestCodec(t)
  secret := codec.decodeEncryptionKey(types.PersistableKey{persisted_key_1})
  persisted := codec.encodeEncryptionKey(types.SecretKey{[]byte(secret_key_1)})
  if bytes.Compare(secret.B, []byte(secret_key_1)) != 0 {
    t.Errorf("Bad persisted key decoding: %x != %x", secret.B, secret_key_1)
  }
  if persisted.S != persisted_key_1 {
    t.Errorf("Bad secret key encoding: %x != %x", persisted.S, persisted_key_1)
  }
  if len(codec.xor_key.B) != len(secret.B) { t.Errorf("Bad xor key") }
}

func TestCodecDefaultKey(t *testing.T) {
  codec := buildTestCodec(t)
  first_persisted := types.PersistableKey{codec.conf.EncryptionKeys[0]}
  first_secret := codec.decodeEncryptionKey(first_persisted)
  first_fp := codec.FingerprintKey(first_secret)
  if codec.cur_fp.S != first_fp.S { t.Errorf("Wrong default fingerprint, must be first in config.") }
}

func TestCreateNewEncryptionKey(t *testing.T) {
  codec := buildTestCodec(t)
  old_fp := codec.CurrentKeyFingerprint()
  old_key,found := codec.keyring[old_fp]
  if !found { t.Fatalf("There is no current key ?!") }

  persisted, err := codec.CreateNewEncryptionKey()
  if err != nil { t.Fatalf("Could not create new key: %v", err) }
  if len(persisted.S) < 1 { t.Errorf("Bad persisted key") }
  if old_fp.S == codec.cur_fp.S || len(codec.cur_fp.S) < 1 {
    t.Errorf("Bad new fingerprint")
  }
  if bytes.Compare(old_key.B, codec.keyring[codec.cur_fp].B) == 0 || len(codec.keyring[codec.cur_fp].B) < 1 {
    t.Errorf("Bad new secret key")
  }
}

func TestReEncryptKeyring(t *testing.T) {
  new_pw := func() ([]byte, error) { return []byte("salut_mrmonkey"), nil }
  expect_plain := types.SecretString{"chocoloco plain text"}
  codec := buildTestCodec(t)
  obfus := codec.EncryptString(expect_plain)
  expect_key_count := len(codec.keyring)

  persisted_keys, err := codec.ReEncryptKeyring(new_pw)
  if err != nil { t.Fatalf("Could not re-encrypt: %v", err) }
  if len(persisted_keys) != expect_key_count { t.Fatalf("Bad number of keys") }

  new_conf := util.LoadTestConf()
  for _,k := range persisted_keys {
    t.Logf("Adding persisted key: %x", k.S)
    new_conf.EncryptionKeys = append(new_conf.EncryptionKeys, k.S)
    for _,old_k := range codec.conf.EncryptionKeys {
      if old_k == k.S { t.Fatalf("Re-encrypted keys are the same: %v", k.S) }
    }
  }
  new_codec, err2 := NewCodecHelper(new_conf, new_pw)
  if err2 != nil { t.Fatalf("Could not create codec: %v", err2) }
  t.Logf("xor key: %x", codec.xor_key)

  plain, err3 := new_codec.DecryptString(types.CurKeyFp, obfus)
  if err3 != nil { t.Fatalf("Could not decrypt: %v", err3) }
  util.EqualsOrFailTest(t, "Bad decryption", plain, expect_plain)
}

func TestSecretToPersistedKey(t *testing.T) {
  codec := buildTestCodec(t)
  persisted, err := codec.CreateNewEncryptionKey()
  if err != nil { t.Fatalf("Could not create new key: %v", err) }
  secret := codec.keyring[codec.CurrentKeyFingerprint()]
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
  fp := codec.FingerprintKey(types.SecretKey{[]byte(secret_key_1)})
  t.Logf("persisted:%s, fingerprint:%s", persisted_key_1, fp)
  if fp.S != fp_persisted_key_1 { t.Errorf("Bad fingerprint calculation: %s != %s", fp, fp_persisted_key_1) }
}

func TestEncryptString(t *testing.T) {
  expect_plain := types.SecretString{"chocoloco plain text"}
  codec := buildTestCodec(t)
  obfus := codec.EncryptString(expect_plain)
  obfus_2 := codec.EncryptString(expect_plain)
  if obfus.S  == obfus_2.S {
    t.Errorf("Encrypt of the same string should not produce the same obfuscated bytes")
  }

  plain, err := codec.DecryptString(types.CurKeyFp, obfus)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  t.Logf("obfuscated:%x, plain:%s", obfus, plain)
  util.EqualsOrFailTest(t, "Bad decryption", plain, expect_plain)
}

func TestEncryptString_Fingerprint(t *testing.T) {
  expect_plain := types.SecretString{"chocoloco plain text"}
  keys := []string {persisted_key_2, persisted_key_1,}
  codec := buildTestCodec(t)
  codec_2 := buildTestCodecChooseEncKey(t, keys)
  fp := codec.FingerprintKey(types.SecretKey{[]byte(secret_key_2)})
  obfus := codec_2.EncryptString(expect_plain)

  plain, err := codec.DecryptString(fp, obfus)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  t.Logf("obfuscated:%x, plain:%s", obfus, plain)
  util.EqualsOrFailTest(t, "Bad decryption", plain, expect_plain)
}

func TestEncryptStream(t *testing.T) {
  codec := buildTestCodec(t)
  expect_msg := []byte("this is some plain text data")
  read_pipe := types.NewMockPreloadedPipe(expect_msg).ReadEnd()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  var err error
  var encoded_pipe, decoded_pipe types.PipeReadEnd
  encoded_pipe, err = codec.EncryptStream(ctx, read_pipe)
  if err != nil { t.Fatalf("Could not encrypt: %v", err) }
  decoded_pipe, err = codec.DecryptStream(ctx, types.CurKeyFp, encoded_pipe)
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
      util.EqualsOrFailTest(t, "Bad encryption", data, expect_msg)
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
  decoded_pipe, err = codec.DecryptStream(ctx, types.CurKeyFp, encoded_pipe)
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
      util.EqualsOrFailTest(t, "Bad encryption len", len(data), 4096*32)
    case <-ctx.Done():
      t.Fatalf("TestEncryptStream timeout")
  }
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

