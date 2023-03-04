package encryption

import (
  "context"
  "crypto/aes"
  "crypto/cipher"
  "crypto/rand"
  "crypto/sha256"
  "crypto/sha512"
  "encoding/base64"
  "fmt"
  "io"
  "reflect"
  "sync"
  "syscall"
  "unsafe"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "golang.org/x/crypto/ssh/terminal"
)

// Keeps key material in global state to ask for passwords just once.
type AesGzipCodecGlobalState struct {
  Mutex   *sync.Mutex
  Keyring map[types.PersistableString]types.SecretKey
  XorKey  types.SecretKey
}
var globalState AesGzipCodecGlobalState

func NewAesGzipCodecGlobalState() *AesGzipCodecGlobalState {
  return &AesGzipCodecGlobalState{
    Mutex: new(sync.Mutex),
    Keyring: make(map[types.PersistableString]types.SecretKey),
    XorKey: types.SecretKey{[]byte("")},
  }
}

func init() {
  globalState = *NewAesGzipCodecGlobalState()
}

// This class uses "Explicit initialization vectors" by prepending a single random block to the plaintext.
// This way the Init Vector does not need to be stored anywhere.
type aesGzipCodec struct {
  conf       *pb.Config
  block_size int
  cur_fp     types.PersistableString
  cur_key    types.SecretKey
}

func NewCodec(conf *pb.Config) (types.Codec, error) {
  return NewCodecHelper(conf, requestPassphrase)
}

func NewCodecHelper(conf *pb.Config, pw_prompt func() ([]byte, error)) (types.Codec, error) {
  codec := &aesGzipCodec{
    conf: conf,
    block_size: aes.BlockSize,
  }
  if err := globalState.DerivatePassphrase(false, pw_prompt); err != nil { return nil, err }
  if len(conf.EncryptionKeys) == 0 {
    return nil, fmt.Errorf("No encryption keys in the configuration")
  }

  for idx,k := range conf.EncryptionKeys {
    dec_key, fp := globalState.DecodeAndAddToKeyring(types.PersistableKey{k})
    // Use the first key in the keyring to encrypt.
    if idx == 0 {
      codec.cur_fp = fp
      codec.cur_key = dec_key
    }
  }
  return codec, nil
}

func requestPassphrase() ([]byte, error) {
  fmt.Print("Enter passphrase to decrypt encryption keys: ")
  // Should NOT copy `byte_pass` because it may contain sensible pw that will stick until GC.
  byte_pass, err := terminal.ReadPassword(int(syscall.Stdin))
  if err != nil { return nil, err }
  if len(byte_pass) < 12 { return nil, fmt.Errorf("Password is too short") }

  err = util.IsOnlyAsciiString(byte_pass, false)
  return byte_pass, err
}

func (self *AesGzipCodecGlobalState) DerivatePassphrase(
    overwrite bool, pw_prompt func() ([]byte, error)) error {
  self.Mutex.Lock()
  defer self.Mutex.Unlock()
  if !overwrite && len(self.XorKey.B) != 0 { return nil }

  passphrase, err := pw_prompt()
  if err != nil { return err }
  raw_key := sha256.Sum256(passphrase)
  self.XorKey = types.SecretKey{raw_key[:]}

  // remove password from memory, not sure how reliable this is...
  for idx,_ := range passphrase { passphrase[idx] = 0 }
  for idx,_ := range passphrase { passphrase[idx] = byte(idx) }
  return nil
}

func (self *AesGzipCodecGlobalState) DecodeAndAddToKeyring(
    enc_str types.PersistableKey) (types.SecretKey, types.PersistableString) {
  self.Mutex.Lock()
  defer self.Mutex.Unlock()

  dec_key := self.decodeEncryptionKey_MustHoldMutex(enc_str)
  fp := FingerprintKey(dec_key)
  // Noop if key is already in globalState.Keyring
  self.Keyring[fp] = dec_key
  return dec_key, fp
}

// Generates a fingerprint for the key that can safely be stored in a non-secure place.
// The key should be impossible to deduce from the fingerprint.
// The fingerprint **must** be issue from a `SecretKey` so that it is not dependent on the method used to encrypt the keys.
func FingerprintKey(key types.SecretKey) types.PersistableString {
  const fp_size = sha512.Size / 4
  if fp_size * 2 > len(key.B) {
    util.Fatalf("Fingerprinting a key that is too small.")
  }
  full_fp := sha512.Sum512(key.B)
  //return fmt.Sprintf("%x", raw_fp)
  raw_fp := base64.StdEncoding.EncodeToString(full_fp[:fp_size])
  return types.PersistableString{raw_fp}
}

func (self *AesGzipCodecGlobalState) decodeEncryptionKey_MustHoldMutex(
    enc_key types.PersistableKey) types.SecretKey {
  if locked := self.Mutex.TryLock(); locked { util.Fatalf("Must hold mutex when calling") }

  enc_bytes, err := base64.StdEncoding.DecodeString(enc_key.S)
  if err != nil { util.Fatalf("Bad key base64 encoding: %v", err) }
  if len(enc_bytes) != len(self.XorKey.B) { util.Fatalf("Bad key length: %v", enc_key) }
  raw_key := make([]byte, len(enc_bytes))
  for idx,b := range enc_bytes {
    raw_key[idx] = b ^ self.XorKey.B[idx]
  }
  return types.SecretKey{raw_key}
}
func TestOnlyDecodeEncryptionKey(enc_key types.PersistableKey) types.SecretKey {
  globalState.Mutex.Lock()
  defer globalState.Mutex.Unlock()
  return globalState.decodeEncryptionKey_MustHoldMutex(enc_key)
}

func (self *AesGzipCodecGlobalState) encodeEncryptionKey_MustHoldMutex(
    dec_key types.SecretKey) types.PersistableKey {
  if locked := self.Mutex.TryLock(); locked { util.Fatalf("Must hold mutex when calling") }
  if len(dec_key.B) != len(self.XorKey.B) { util.Fatalf("Bad key length") }

  enc_bytes := make([]byte, len(dec_key.B))
  for idx,b := range dec_key.B {
    enc_bytes[idx] = b ^ self.XorKey.B[idx]
  }
  enc_str := base64.StdEncoding.EncodeToString(enc_bytes)
  return types.PersistableKey{enc_str}
}
func TestOnlyEncodeEncryptionKey(dec_key types.SecretKey) types.PersistableKey {
  globalState.Mutex.Lock()
  defer globalState.Mutex.Unlock()
  return globalState.encodeEncryptionKey_MustHoldMutex(dec_key)
}

func (self *AesGzipCodecGlobalState) AddToKeyringThenEncode(
    dec_key types.SecretKey) (types.PersistableKey, types.PersistableString, error) {
  self.Mutex.Lock()
  defer self.Mutex.Unlock()
  fp := FingerprintKey(dec_key)
  if _,found := self.Keyring[fp]; found {
    null_fp := types.PersistableString{""}
    null_key := types.PersistableKey{""}
    return null_key, null_fp, fmt.Errorf("Fingerprint duplicated: '%s'", fp)
  }
  enc_key := self.encodeEncryptionKey_MustHoldMutex(dec_key)
  self.Keyring[fp] = dec_key
  return enc_key, fp, nil
}

func (self *AesGzipCodecGlobalState) Get(fp types.PersistableString) (types.SecretKey, error) {
  self.Mutex.Lock()
  defer self.Mutex.Unlock()
  if dec_key,found := self.Keyring[fp]; !found {
    null_key := types.SecretKey{[]byte("")}
    return null_key, fmt.Errorf("Fingerprint not found: '%s'", fp)
  } else {
    return dec_key, nil
  }
}

func TestOnlyKeyCount() int {
  globalState.Mutex.Lock()
  defer globalState.Mutex.Unlock()
  return len(globalState.Keyring)
}

func TestOnlyFlush() {
  globalState.Mutex.Lock()
  defer globalState.Mutex.Unlock()
  globalState.Keyring = make(map[types.PersistableString]types.SecretKey)
  globalState.XorKey = types.SecretKey{[]byte("")}
}

func (self *AesGzipCodecGlobalState) EncodeAllInKeyring(
    first_fp types.PersistableString) ([]types.PersistableKey, error) {
  self.Mutex.Lock()
  defer self.Mutex.Unlock()
  persisted_keys := make([]types.PersistableKey, 0, len(self.Keyring))

  // Put the current encryption key first.
  dec_key, found := self.Keyring[first_fp]
  if !found { return nil, fmt.Errorf("keyring invalid state.") }
  persisted_keys = append(persisted_keys, self.encodeEncryptionKey_MustHoldMutex(dec_key))

  for fp,dec_key := range self.Keyring {
    if fp.S == first_fp.S { continue }
    persisted_keys = append(persisted_keys, self.encodeEncryptionKey_MustHoldMutex(dec_key))
  }
  return persisted_keys, nil
}

func (self *aesGzipCodec) EncryptionHeaderLen() int { return self.block_size }

func (self *aesGzipCodec) CreateNewEncryptionKey() (types.PersistableKey, error) {
  const AES_256_KEY_LEN = 32
  null_key := types.PersistableKey{""}

  raw_key := make([]byte, AES_256_KEY_LEN)
  _, err := rand.Read(raw_key)
  if err != nil { util.Fatalf("Could not generate random key: %v", err) }
  dec_key := types.SecretKey{raw_key}

  enc_key, fp, err := globalState.AddToKeyringThenEncode(dec_key)
  if err != nil { return null_key, err }
  self.cur_fp = fp
  self.cur_key = dec_key
  return enc_key, nil
}

func (self *aesGzipCodec) CurrentKeyFingerprint() types.PersistableString {
  return self.cur_fp
}

func (self *aesGzipCodec) ReEncryptKeyring(
    pw_prompt func() ([]byte, error)) ([]types.PersistableKey, error) {
  if err := globalState.DerivatePassphrase(true, pw_prompt); err != nil { return nil, err }
  return globalState.EncodeAllInKeyring(self.CurrentKeyFingerprint())
}

func NoCopyByteSliceToString(bytes []byte) string {
	hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	return *(*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
	}))
}

func NoCopyStringToByteSlice(str string) []byte {
	hdr := *(*reflect.StringHeader)(unsafe.Pointer(&str))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
		Cap:  hdr.Len,
	}))
}

func (self *aesGzipCodec) getStreamEncrypter() cipher.Stream {
  iv := make([]byte, self.block_size)
  if _, err := io.ReadFull(rand.Reader, iv); err != nil {
    util.Fatalf("IV failed; %v", err)
  }
  block, err := aes.NewCipher(self.cur_key.B)
  if err != nil { util.Fatalf("aes.NewCipher: %v", err) }
  return cipher.NewCFBEncrypter(block, iv)
}

func (self *aesGzipCodec) getStreamDecrypter(key_fp types.PersistableString) (cipher.Stream, error) {
  var block cipher.Block
  var err error
  if len(key_fp.S) == 0 || key_fp.S == self.cur_fp.S {
    block, err = aes.NewCipher(self.cur_key.B)
  } else {
    dec_key, err := globalState.Get(key_fp)
    if err != nil { return nil, err }
    block, err = aes.NewCipher(dec_key.B)
  }
  if err != nil { return nil, err }
  null_iv := make([]byte, self.block_size)
  return cipher.NewCFBDecrypter(block, null_iv), nil
}

func (self *aesGzipCodec) EncryptString(clear types.SecretString) types.PersistableString {
  null_first_block := make([]byte, self.block_size)
  padded_obfus := make([]byte, len(clear.S) + self.block_size)
  stream := self.getStreamEncrypter()
  stream.XORKeyStream(padded_obfus, null_first_block)
  stream.XORKeyStream(padded_obfus[self.block_size:], NoCopyStringToByteSlice(clear.S))
  return types.PersistableString{base64.StdEncoding.EncodeToString(padded_obfus)}
}

func (self *aesGzipCodec) DecryptString(key_fp types.PersistableString, obfus types.PersistableString) (types.SecretString, error) {
  null_str := types.SecretString{""}
  obfus_bytes, err_dec := base64.StdEncoding.DecodeString(obfus.S)
  if err_dec != nil { return null_str, err_dec }

  padded_plain := make([]byte, len(obfus_bytes))
  stream, err := self.getStreamDecrypter(key_fp)
  if err != nil { return null_str, err }
  if len(obfus_bytes) < self.block_size {
    return null_str, fmt.Errorf("Obfuscated string is too short, expecting some larger than 1 block")
  }
  stream.XORKeyStream(padded_plain, obfus_bytes)
  plain := NoCopyByteSliceToString(padded_plain[self.block_size:])
  return types.SecretString{plain}, nil
}

func (self *aesGzipCodec) EncryptStream(
    ctx context.Context, input types.ReadEndIf) (types.ReadEndIf, error) {
  pipe := util.NewInMemPipe(ctx)
  defer func() { util.OnlyCloseWriteEndWhenError(pipe, input.GetErr()) }()
  stream := self.getStreamEncrypter()
  block_buffer := make([]byte, 128 * self.block_size)

  go func() {
    var err error
    defer func() { util.CloseWriteEndWithError(pipe, util.Coalesce(input.GetErr(), err)) }()
    defer func() { util.CloseWithError(input, err) }()
    done := false

    first_block := block_buffer[0:self.block_size]
    // it is valid to reuse slice for output if offsets are the same
    stream.XORKeyStream(first_block, first_block)
    _, err = pipe.WriteEnd().Write(first_block)
    if err != nil { return }

    for !done && err == nil && ctx.Err() == nil {
      var count int
      count, err = input.Read(block_buffer)
      if err != nil && err != io.EOF { return }
      if count == 0 && err == nil { continue }
      if count == 0 && err == io.EOF { err = nil; return }
      done = (err == io.EOF)

      stream.XORKeyStream(block_buffer[:count], block_buffer[:count])
      _, err = pipe.WriteEnd().Write(block_buffer[:count])
      //util.Debugf("encrypt count=%d done=%v bytes=%x", count, done, block_buffer[:count])
    }
  }()
  return pipe.ReadEnd(), input.GetErr()
}

func (self *aesGzipCodec) decryptBlock_Helper(buffer []byte, stream cipher.Stream, input io.Reader, output io.Writer) (bool, int, error) {
  count, err := input.Read(buffer)
  if err != nil && err != io.EOF {
    return true, count, fmt.Errorf("DecryptStream failed reading: %v", err)
  }
  if count == 0 && err == nil { return false, 0, nil }
  if count == 0 && err == io.EOF { return true, 0, nil }
  // it is valid to reuse slice for output if offsets are the same
  stream.XORKeyStream(buffer, buffer)

  _, err = output.Write(buffer[:count])
  if err != nil { return true, count, err }
  //util.Debugf("decrypt count=%d done=%v bytes=%x", count, done, buffer[:count])
  return (err == io.EOF), count, nil
}

func (self *aesGzipCodec) decryptStream_BlockIterator(
    ctx context.Context, stream cipher.Stream, input io.Reader, output io.Writer) error {
  var err error
  var count int
  var done bool
  block_buffer := make([]byte, 128 * self.block_size)

  first_block := block_buffer[0:self.block_size]
  done, count, err = self.decryptBlock_Helper(first_block, stream, input, io.Discard)
  // The first block should always be there, if we get EOF something went really wrong.
  if err != nil || done || count != len(first_block) {
    return fmt.Errorf("First block not written correctly: %v", err)
  }

  for !done && err == nil && ctx.Err() == nil {
    done, _, err = self.decryptBlock_Helper(block_buffer, stream, input, output)
  }
  return ctx.Err()
}

func (self *aesGzipCodec) DecryptStream(
    ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf) (types.ReadEndIf, error) {
  var err error
  pipe := util.NewFileBasedPipe(ctx)
  defer func() { util.OnlyCloseWriteEndWhenError(pipe, util.Coalesce(input.GetErr(), err)) }()
  defer func() { util.OnlyCloseWhenError(input, err) }()

  stream, err := self.getStreamDecrypter(key_fp)
  if err != nil { return nil, err }

  go func() {
    var err error
    defer func() { util.CloseWriteEndWithError(pipe, util.Coalesce(input.GetErr(), err)) }()
    defer func() { util.CloseWithError(input, err) }()
    err = self.decryptStream_BlockIterator(ctx, stream, input, pipe.WriteEnd())
  }()
  return pipe.ReadEnd(), input.GetErr()
}

func (self *aesGzipCodec) DecryptStreamLeaveSinkOpen(
    ctx context.Context, key_fp types.PersistableString, input types.ReadEndIf, output io.WriteCloser) error {
  var err error
  defer func() { util.CloseWithError(input, err) }()
  // We do not close on purpose, so that `output` can contain the chained streams from multiple calls.
  defer func() { util.OnlyCloseWhenError(output, err) }()

  stream, err := self.getStreamDecrypter(key_fp)
  if err != nil { return err }

  err = self.decryptStream_BlockIterator(ctx, stream, input, output)
  return util.Coalesce(input.GetErr(), err)
}

