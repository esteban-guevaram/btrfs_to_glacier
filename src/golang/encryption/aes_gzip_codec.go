package encryption

import (
  "context"
  "crypto/aes"
  "crypto/cipher"
  "crypto/md5"
  "crypto/rand"
  "crypto/sha256"
  "encoding/base64"
  "fmt"
  "io"
  "reflect"
  "syscall"
  "unicode"
  "unicode/utf8"
  "unsafe"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "golang.org/x/crypto/ssh/terminal"
)

// This class uses "Explicit initialization vectors" by prepending a single random block to the plaintext.
// This way the Init Vector does not need to be stoed anywhere.
type aesGzipCodec struct {
  conf    *pb.Config
  keyring map[string]types.SecretKey
  xor_key types.SecretKey
  block   cipher.Block
}

func NewCodec(conf *pb.Config) (types.Codec, error) {
  return NewCodecHelper(conf, requestPassphrase)
}

func NewCodecHelper(conf *pb.Config, pw_prompt func() (string, error)) (types.Codec, error) {
  codec := &aesGzipCodec{
    conf: conf,
    keyring: make(map[string]types.SecretKey),
  }
  passphrase, err := pw_prompt()
  if err != nil { return nil, err }
  codec.xor_key = derivatePassphrase(passphrase)

  if len(conf.EncryptionKeys) == 0 {
    return nil, fmt.Errorf("No encryption keys in the configuration")
  }
  for _,k := range conf.EncryptionKeys {
    enc_key := types.PersistableKey{k}
    fp := codec.FingerprintKey(enc_key)
    if _,found := codec.keyring[fp]; found {
      return nil, fmt.Errorf("Fingerprint duplicated: '%s'", fp)
    }
    codec.keyring[fp] = codec.decodeEncryptionKey(enc_key)
    // Use the first key in the keyring to encrypt.
    if codec.block == nil {
      codec.block, err = aes.NewCipher(codec.keyring[fp].B)
      if err != nil { return nil, err }
    }
  }
  return codec, nil
}

func requestPassphrase() (string, error) {
  fmt.Print("Enter passphrase to decrypt encryption keys: ")
  byte_pass, err := terminal.ReadPassword(int(syscall.Stdin))
  if err != nil { return "", err }
  if len(byte_pass) < 8 { return "", fmt.Errorf("Password is too short") }

  pass := string(byte_pass)
  if !utf8.ValidString(pass) { return "", fmt.Errorf("Password is not valid unicode") }
  for _,codept := range pass {
    if codept > unicode.MaxASCII || unicode.IsControl(codept) {
      return "", fmt.Errorf("Password is has invalid characters (only ascii non control are allowed)")
    }
  }
  return pass, nil
}

func derivatePassphrase(passphrase string) types.SecretKey {
  raw_key := sha256.Sum256([]byte(passphrase))
  return types.SecretKey{raw_key[:]}
}

func (self *aesGzipCodec) decodeEncryptionKey(enc_key types.PersistableKey) types.SecretKey {
  enc_bytes, err := base64.StdEncoding.DecodeString(enc_key.S)
  if err != nil { util.Fatalf("Bad key base64 encoding: %v", err) }
  if len(enc_bytes) != len(self.xor_key.B) { util.Fatalf("Bad key length: %v", enc_key) }
  raw_key := make([]byte, len(enc_bytes))
  for idx,b := range enc_bytes {
    raw_key[idx] = b ^ self.xor_key.B[idx]
  }
  return types.SecretKey{raw_key}
}
func (self *aesGzipCodec) encodeEncryptionKey(enc_key types.SecretKey) types.PersistableKey {
  str_key := types.PersistableKey{ base64.StdEncoding.EncodeToString(enc_key.B) }
  enc_bytes := self.decodeEncryptionKey(str_key).B
  return types.PersistableKey{ base64.StdEncoding.EncodeToString(enc_bytes) }
}

// Generates a random encryption key of a suitable length for the encryption algo.
// Then it derives a second key of the same length from `self.passphrase`.
// That second key is ORed with the first and encoded into a string that can be stored in the config.
// Neither `passphrase` nor the first encryption key should be persisted anywhere.
func (self *aesGzipCodec) GenerateEncryptionKey() (types.SecretKey, types.PersistableKey) {
  const AES_256_KEY_LEN = 32
  raw_key := make([]byte, AES_256_KEY_LEN)
  _, err := rand.Read(raw_key)
  if err != nil { util.Fatalf("Could not generate random key: %v", err) }
  secret := types.SecretKey{raw_key}
  return secret, self.encodeEncryptionKey(secret)
}

func (self *aesGzipCodec) FingerprintKey(key types.PersistableKey) string {
  raw_fp := md5.Sum([]byte(key.S))
  //return fmt.Sprintf("%x", raw_fp)
  return base64.StdEncoding.EncodeToString(raw_fp[:])
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
  iv := make([]byte, self.block.BlockSize())
  if _, err := io.ReadFull(rand.Reader, iv); err != nil {
    util.Fatalf("IV failed; %v", err)
  }
  return cipher.NewCFBEncrypter(self.block, iv)
}

func (self *aesGzipCodec) getStreamDecrypter(key_fp string) (cipher.Block, cipher.Stream, error) {
  block := self.block
  if len(key_fp) > 0 {
    k, found := self.keyring[key_fp]
    if !found  {
      return nil, nil, fmt.Errorf("%s does not exist in the keyring", key_fp)
    }
    var err error
    block, err = aes.NewCipher(k.B)
    if err != nil { return nil, nil, err }
  }
  null_iv := make([]byte, block.BlockSize())
  return block, cipher.NewCFBDecrypter(block, null_iv), nil
}

func (self *aesGzipCodec) EncryptString(clear string) []byte {
  null_first_block := make([]byte, self.block.BlockSize())
  padded_obfus := make([]byte, len(clear) + self.block.BlockSize())
  stream := self.getStreamEncrypter()
  stream.XORKeyStream(padded_obfus, null_first_block)
  stream.XORKeyStream(padded_obfus[self.block.BlockSize():], NoCopyStringToByteSlice(clear))
  return padded_obfus
}

func (self *aesGzipCodec) DecryptString(key_fp string, obfus []byte) (string, error) {
  padded_plain := make([]byte, len(obfus))
  block, stream, err := self.getStreamDecrypter(key_fp)
  if err != nil { return "", err }
  if len(obfus) < self.block.BlockSize() {
    return "", fmt.Errorf("Obfuscated string is too short, expecting some larger than 1 block")
  }
  stream.XORKeyStream(padded_plain, obfus)
  plain := NoCopyByteSliceToString(padded_plain[block.BlockSize():])
  return plain, nil
}

func (self *aesGzipCodec) EncryptStream(ctx context.Context, input types.PipeReadEnd) (types.PipeReadEnd, error) {
  if input.GetErr() != nil {
    return nil, fmt.Errorf("EncryptStream input has an error")
  }
  var err error
  pipe := util.NewFileBasedPipe()
  defer util.CloseIfProblemo(pipe, &err)

  stream := self.getStreamEncrypter()
  block_buffer := make([]byte, 128 * self.block.BlockSize())
  first_block := block_buffer[0:self.block.BlockSize()]

  // it is valid to reuse slice for output if offsets are the same
  stream.XORKeyStream(first_block, first_block)
  // The file pipe should not block writing `first_block` there is not enough data (I hope)
  _, err = pipe.WriteEnd().Write(first_block)
  if err != nil { return nil, err }

  go func() {
    done := false
    writer := pipe.WriteEnd()
    defer writer.Close()
    defer input.Close()

    for !done && ctx.Err() == nil {
      count, err := input.Read(block_buffer)
      if err != nil && err != io.EOF {
        writer.PutErr(fmt.Errorf("EncryptStream failed reading"))
        return
      }
      done = (err == io.EOF)

      stream.XORKeyStream(block_buffer[:count], block_buffer[:count])
      _, err = writer.Write(block_buffer[:count])
      if err != nil {
        writer.PutErr(fmt.Errorf("EncryptStream failed writing: %v", err))
        return
      }
      //util.Debugf("encrypt count=%d done=%v bytes=%x", count, done, block_buffer[:count])
    }
  }()
  return pipe.ReadEnd(), nil
}

func (self *aesGzipCodec) DecryptStream(ctx context.Context, key_fp string, input types.PipeReadEnd) (types.PipeReadEnd, error) {
  if input.GetErr() != nil {
    return nil, fmt.Errorf("DecryptStream input has an error")
  }
  var err error
  pipe := util.NewFileBasedPipe()
  defer util.CloseIfProblemo(pipe, &err)

  block, stream, err := self.getStreamDecrypter(key_fp)
  if err != nil { return nil, err }

  decrypt_helper := func(buffer []byte) (bool, int, error) {
    count, err := input.Read(buffer)
    if err != nil && err != io.EOF {
      return true, count, fmt.Errorf("DecryptStream failed reading")
    }
    // it is valid to reuse slice for output if offsets are the same
    stream.XORKeyStream(buffer, buffer)
    done := (err == io.EOF)
    return done, count, nil
  }

  go func() {
    var err error
    done := false
    block_buffer := make([]byte, 128 * block.BlockSize())
    writer := pipe.WriteEnd()
    defer writer.Close()
    defer input.Close()

    first_block := block_buffer[0:block.BlockSize()]
    done, _, err = decrypt_helper(first_block)
    if err != nil && err != io.EOF {
      writer.PutErr(err)
      return
    }

    for !done && ctx.Err() == nil {
      var count int
      done, count, err = decrypt_helper(block_buffer)

      _, err = writer.Write(block_buffer[:count])
      if err != nil {
        writer.PutErr(fmt.Errorf("DecryptStream failed writing: %v", err))
        return
      }
      //util.Debugf("decrypt count=%d done=%v bytes=%x", count, done, block_buffer[:count])
    }
  }()
  return pipe.ReadEnd(), nil
}

