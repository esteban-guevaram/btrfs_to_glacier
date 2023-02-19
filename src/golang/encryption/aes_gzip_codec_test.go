package encryption

import (
  "bytes"
  "compress/gzip"
  "context"
  "fmt"
  "io"
  "testing"
  "time"
  //pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
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

type source_t struct { io.ReadCloser ; closed bool }
func (self *source_t) Close() error { self.closed = true; return nil }
func (self *source_t) GetErr() error { return nil }
func TestEncryptStream_ClosesStreams(t *testing.T) {
  codec := buildTestCodec(t)
  read_pipe := &source_t{ ReadCloser:io.NopCloser(bytes.NewReader([]byte("coucou"))), }
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  encoded_pipe, err := codec.EncryptStream(ctx, read_pipe)
  if err != nil { t.Fatalf("Could not encrypt: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer encoded_pipe.Close()
    _, err := io.ReadAll(encoded_pipe) // only returns if codec closes the write end.
    if err != nil { t.Errorf("ReadAll: %v", err) }
  }()

  util.WaitForClosure(t, ctx, done)
  if !read_pipe.closed { t.Errorf("source not closed") }
}

type decrypt_f = func(types.Codec, context.Context, types.PersistableString, types.ReadEndIf) (types.ReadEndIf, error)
func testEncryptDecryptStream_Helper(
    t *testing.T, read_pipe types.ReadEndIf, decrypt_lambda decrypt_f) []byte {
  codec := buildTestCodec(t)
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  var err error
  var encoded_pipe, decoded_pipe types.ReadEndIf
  encoded_pipe, err = codec.EncryptStream(ctx, read_pipe)
  if err != nil { t.Fatalf("Could not encrypt: %v", err) }
  decoded_pipe, err = decrypt_lambda(codec, ctx, types.CurKeyFp, encoded_pipe)
  if err != nil { t.Fatalf("Could not decrypt: %v", err) }

  done := make(chan []byte)
  go func() {
    defer close(done)
    defer decoded_pipe.Close()
    data, err := io.ReadAll(decoded_pipe)
    if err != nil { t.Errorf("ReadAll: %v", err) }
    done <- data
  }()

  var data []byte
  select {
    case data = <-done:
    case <-ctx.Done(): t.Fatalf("testEncryptDecryptStream_Helper timeout")
  }
  return data
}

func TestEncryptStream(t *testing.T) {
  expect_msg := []byte("this is some plain text data")
  read_pipe := mocks.NewPreloadedPipe(expect_msg).ReadEnd()
  decrypt_lambda := func(
      codec types.Codec, ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf) (types.ReadEndIf, error) {
    return codec.DecryptStream(ctx, key_fp, input)
  }
  data := testEncryptDecryptStream_Helper(t, read_pipe, decrypt_lambda)
  util.EqualsOrFailTest(t, "Bad encryption", data, expect_msg)
}

func TestEncryptStreamInto_SmallMsg(t *testing.T) {
  expect_msg := []byte("this is some plain text data")
  read_pipe := mocks.NewPreloadedPipe(expect_msg).ReadEnd()
  decrypt_lambda := func(
      codec types.Codec, ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf) (types.ReadEndIf, error) {
    pipe := util.NewInMemPipe(ctx)
    go func() {
      defer pipe.WriteEnd().Close()
      err := codec.DecryptStreamLeaveSinkOpen(ctx, key_fp, input, pipe.WriteEnd())
      if err != nil { util.Fatalf("codec.DecryptStreamLeaveSinkOpen: %v", err) }
    }()
    return pipe.ReadEnd(), nil
  }
  data := testEncryptDecryptStream_Helper(t, read_pipe, decrypt_lambda)
  util.EqualsOrFailTest(t, "Bad encryption", data, expect_msg)
}

func TestEncryptStream_MoreData(t *testing.T) {
  read_pipe := util.ProduceRandomTextIntoPipe(context.TODO(), 4096, 32)
  decrypt_lambda := func(
      codec types.Codec, ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf) (types.ReadEndIf, error) {
    return codec.DecryptStream(ctx, key_fp, input)
  }
  data := testEncryptDecryptStream_Helper(t, read_pipe, decrypt_lambda)
  util.EqualsOrFailTest(t, "Bad encryption len", len(data), 4096*32)
}

func TestEncryptStreamInto_MoreData(t *testing.T) {
  read_pipe := util.ProduceRandomTextIntoPipe(context.TODO(), 4096, 32)
  decrypt_lambda := func(
      codec types.Codec, ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf) (types.ReadEndIf, error) {
    pipe := util.NewInMemPipe(ctx)
    go func() {
      defer pipe.WriteEnd().Close()
      err := codec.DecryptStreamLeaveSinkOpen(ctx, key_fp, input, pipe.WriteEnd())
      if err != nil { util.Fatalf("codec.DecryptStreamLeaveSinkOpen: %v", err) }
    }()
    return pipe.ReadEnd(), nil
  }
  data := testEncryptDecryptStream_Helper(t, read_pipe, decrypt_lambda)
  util.EqualsOrFailTest(t, "Bad encryption len", len(data), 4096*32)
}

func TestDecryptStreamLeaveSinkOpen_OutputRemainsOpen(t *testing.T) {
  read_pipe := util.ProduceRandomTextIntoPipe(context.TODO(), 32, 1)
  pipe := util.NewFileBasedPipe(context.TODO()) // we use a file to avoid blocking on the last write
  decrypt_lambda := func(
      codec types.Codec, ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf) (types.ReadEndIf, error) {
    reader := util.NewLimitedReadEnd(pipe.ReadEnd(), 32) 
    codec.DecryptStreamLeaveSinkOpen(ctx, key_fp, input, pipe.WriteEnd())
    return reader, nil
  }
  testEncryptDecryptStream_Helper(t, read_pipe, decrypt_lambda)
  select { case <-time.After(util.SmallTimeout): }
  _, err := pipe.WriteEnd().Write([]byte("coucou"))
  if err != nil { t.Errorf("could not write after decryption: %v", err) }
}

type streamf_t = func(context.Context, types.ReadEndIf) (types.ReadEndIf, error)
func testStream_TimeoutContinousReadUntilDeadline_Helper(t *testing.T, stream_f streamf_t) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  read_pipe := util.ProduceRandomTextIntoPipe(context.TODO(), 4096, /*infinite*/0)

  timely_close := make(chan bool)

  out, err := stream_f(ctx, read_pipe)
  if err != nil { t.Fatalf("Could process stream: %v", err) }
  go func() {
    buf := make([]byte, 32)
    defer out.Close()
    defer close(timely_close)
    for {
      _,err := out.Read(buf)
      if err != nil { return }
    }
  }()

  select {
    case <-timely_close:
    case <-time.After(util.LargeTimeout):
      t.Fatalf("codec did not react to context timeout: %v", ctx.Err())
  }
}

func testStream_TimeoutReadAfterClose_Helper(t *testing.T, stream_f streamf_t) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  read_pipe := util.ProduceRandomTextIntoPipe(context.TODO(), 4096, /*infinite*/0)

  timely_close := make(chan bool)
  out, err := stream_f(ctx, read_pipe)
  if err != nil { t.Fatalf("Could process stream: %v", err) }

  go func() {
    defer close(timely_close)
    defer out.Close()
    buf := make([]byte, 32)
    select { case <-ctx.Done(): }
    select { case <-time.After(util.SmallTimeout): out.Read(buf) }
  }()

  // Wait until we are done
  select {
    case <-timely_close: return
    case <-time.After(util.LargeTimeout):
      t.Fatalf("codec did not react to context timeout.")
  }
}

func TestEncryptStream_TimeoutContinousReads(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    return codec.EncryptStream(ctx, in)
  }
  testStream_TimeoutContinousReadUntilDeadline_Helper(t, stream_f)
}

func TestDecryptStream_TimeoutContinousReads(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    return codec.DecryptStream(ctx, types.CurKeyFp, in)
  }
  testStream_TimeoutContinousReadUntilDeadline_Helper(t, stream_f)
}

func TestDecryptStreamLeaveSinkOpen_TimeoutBlockingWrite(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    write_end := util.NewWriteEndBlock(ctx, util.TestTimeout * 2)
    err := codec.DecryptStreamLeaveSinkOpen(ctx, types.CurKeyFp, in, write_end)
    if err == nil { util.Fatalf("codec.DecryptStreamLeaveSinkOpen should return err on blocking write") }
    return util.ReadEndFromBytes(nil), nil
  }
  testStream_TimeoutContinousReadUntilDeadline_Helper(t, stream_f)
}

func TestDecryptStreamLeaveSinkOpen_TimeoutContinousReads(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    pipe := util.NewInMemPipe(ctx)
    go func() {
      if ctx.Err() != nil { util.Fatalf("wtf") }
      if codec.DecryptStreamLeaveSinkOpen(ctx, types.CurKeyFp, in, pipe.WriteEnd()) == nil {
        util.Fatalf("codec.DecryptStreamLeaveSinkOpen should return err, ctx: %v", ctx.Err())
      }
    }()
    return pipe.ReadEnd(), nil
  }
  testStream_TimeoutContinousReadUntilDeadline_Helper(t, stream_f)
}

func TestEncryptStream_TimeoutReadAfterClose(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    return codec.EncryptStream(ctx, in)
  }
  testStream_TimeoutReadAfterClose_Helper(t, stream_f)
}

func TestDecryptStream_TimeoutReadAfterClose(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    return codec.DecryptStream(ctx, types.CurKeyFp, in)
  }
  testStream_TimeoutReadAfterClose_Helper(t, stream_f)
}

func TestDecryptStreamLeaveSinkOpen_TimeoutReadAfterClose(t *testing.T) {
  codec := buildTestCodec(t)
  stream_f := func(ctx context.Context, in types.ReadEndIf) (types.ReadEndIf, error) {
    pipe := util.NewInMemPipe(ctx)
    go func() {
      err := codec.DecryptStreamLeaveSinkOpen(ctx, types.CurKeyFp, in, pipe.WriteEnd())
      if err == nil { util.Fatalf("codec.DecryptStreamLeaveSinkOpen reading after timeout should return error") }
    }()
    return pipe.ReadEnd(), nil
  }
  testStream_TimeoutReadAfterClose_Helper(t, stream_f)
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

func HelperEncryptStream_ErrPropagation(t *testing.T, err_injector func(types.Pipe)) {
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  codec := buildTestCodec(t)
  pipes := []types.Pipe{
    mocks.NewPreloadedPipe(util.GenerateRandomTextData(24)),
    util.NewFileBasedPipe(ctx),
    util.NewInMemPipe(ctx),
  }
  for _,pipe := range pipes {
    err_injector(pipe)
    encoded_pipe, err := codec.EncryptStream(ctx, pipe.ReadEnd())
    if err != nil { t.Logf("Could not encrypt: %v", err) }

    done := make(chan error)
    go func() {
      defer close(done)
      defer encoded_pipe.Close()
      data,_ := io.ReadAll(encoded_pipe)
      if encoded_pipe.GetErr() == nil { t.Errorf("Expected error propagation") }
      // Cannot guarantee an error will prevent garbage to be written
      if len(data) > codec.EncryptionHeaderLen() {
        t.Logf("Wrote %d bytes despite error input", len(data))
      }
    }()
    util.WaitForClosure(t, ctx, done)
  }
}
func TestEncryptStream_PrematurelyClosedInput(t *testing.T) {
  err_injector := func(p types.Pipe) { p.ReadEnd().Close() }
  HelperEncryptStream_ErrPropagation(t, err_injector)
}
func TestEncryptStream_WriteEndError(t *testing.T) {
  err_injector := func(p types.Pipe) { p.WriteEnd().SetErr(fmt.Errorf("inject_err")) }
  HelperEncryptStream_ErrPropagation(t, err_injector)
}

func HelperDecryptStream_ErrPropagation(t *testing.T, err_injector func(types.Pipe)) {
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  codec := buildTestCodec(t)
  pipes := []types.Pipe{
    mocks.NewPreloadedPipe(util.GenerateRandomTextData(99)),
    util.NewFileBasedPipe(ctx),
    util.NewInMemPipe(ctx),
  }
  for _,pipe := range pipes {
    err_injector(pipe)
    decoded_pipe, err := codec.DecryptStream(ctx, types.CurKeyFp, pipe.ReadEnd())
    if err != nil { t.Logf("Could not decrypt: %v", err) }

    done := make(chan error)
    go func() {
      defer close(done)
      defer decoded_pipe.Close()
      data,_ := io.ReadAll(decoded_pipe)
      if decoded_pipe.GetErr() == nil { t.Errorf("Expected error propagation") }
      // Cannot guarantee an error will prevent garbage to be written
      if len(data) > 0 { t.Logf("Wrote %d bytes despite closed input", len(data)) }
    }()
    util.WaitForClosure(t, ctx, done)
  }
}
func TestDecryptStream_PrematurelyClosedInput(t *testing.T) {
  err_injector := func(p types.Pipe) { p.ReadEnd().Close() }
  HelperDecryptStream_ErrPropagation(t, err_injector)
}
func TestDecryptStream_WriteEndError(t *testing.T) {
  err_injector := func(p types.Pipe) { p.WriteEnd().SetErr(fmt.Errorf("inject_err")) }
  HelperDecryptStream_ErrPropagation(t, err_injector)
}

func HelperDecryptStreamLeaveSinkOpen_ErrPropagation(t *testing.T, err_injector func(types.Pipe)) {
  ctx,cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  sink := util.NewBufferWriteEnd()
  pipe := mocks.NewPreloadedPipe(util.GenerateRandomTextData(24))
  err_injector(pipe)
  codec := buildTestCodec(t)

  err := codec.DecryptStreamLeaveSinkOpen(ctx, types.CurKeyFp, pipe.ReadEnd(), sink)
  if err == nil { t.Errorf("Expected error propagation: %v", err) }
  // Cannot guarantee an error will prevent garbage to be written
  if sink.Len() > 0 { t.Logf("Wrote %d bytes despite closed input", sink.Len()) }
}
func TestDecryptStreamLeaveSinkOpen_PrematurelyClosedInput(t *testing.T) {
  err_injector := func(p types.Pipe) { p.ReadEnd().Close() }
  HelperDecryptStreamLeaveSinkOpen_ErrPropagation(t, err_injector)
}
func TestDecryptStreamLeaveSinkOpen_WriteEndError(t *testing.T) {
  err_injector := func(p types.Pipe) { p.WriteEnd().SetErr(fmt.Errorf("inject_err")) }
  HelperDecryptStreamLeaveSinkOpen_ErrPropagation(t, err_injector)
}

