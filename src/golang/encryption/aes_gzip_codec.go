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
// This way the Init Vector does not need to be stored anywhere.
type aesGzipCodec struct {
  conf    *pb.Config
  keyring map[types.PersistableString]types.SecretKey
  xor_key types.SecretKey
  block   cipher.Block
  cur_fp  types.PersistableString
}

func NewCodec(conf *pb.Config) (types.Codec, error) {
  return NewCodecHelper(conf, requestPassphrase)
}

func NewCodecHelper(conf *pb.Config, pw_prompt func() ([]byte, error)) (types.Codec, error) {
  codec := &aesGzipCodec{
    conf: conf,
    keyring: make(map[types.PersistableString]types.SecretKey),
  }
  var err error
  codec.xor_key, err = derivatePassphrase(pw_prompt)
  if err != nil { return nil, err }

  if len(conf.EncryptionKeys) == 0 {
    return nil, fmt.Errorf("No encryption keys in the configuration")
  }
  for _,k := range conf.EncryptionKeys {
    enc_key := types.PersistableKey{k}
    dec_key := codec.decodeEncryptionKey(enc_key)
    fp := codec.FingerprintKey(dec_key)
    if _,found := codec.keyring[fp]; found {
      return nil, fmt.Errorf("Fingerprint duplicated: '%s'", fp)
    }
    codec.keyring[fp] = dec_key
    // Use the first key in the keyring to encrypt.
    if codec.block == nil {
      codec.block, err = aes.NewCipher(codec.keyring[fp].B)
      if err != nil { return nil, err }
      codec.cur_fp = fp
    }
  }
  return codec, nil
}

func requestPassphrase() ([]byte, error) {
  fmt.Print("Enter passphrase to decrypt encryption keys: ")
  byte_pass, err := terminal.ReadPassword(int(syscall.Stdin))
  if err != nil { return nil, err }
  if len(byte_pass) < 12 { return nil, fmt.Errorf("Password is too short") }

  pass := string(byte_pass)
  if !utf8.ValidString(pass) { return nil, fmt.Errorf("Password is not valid unicode") }
  for _,codept := range pass {
    if codept > unicode.MaxASCII || unicode.IsControl(codept) {
      return nil, fmt.Errorf("Password is has invalid characters (only ascii non control are allowed)")
    }
  }
  return byte_pass, nil
}

func derivatePassphrase(pw_prompt func() ([]byte, error)) (types.SecretKey, error) {
  null_key := types.SecretKey{[]byte("")}
  passphrase, err := pw_prompt()
  if err != nil { return null_key, err }
  raw_key := sha256.Sum256(passphrase)
  return types.SecretKey{raw_key[:]}, nil
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

func (self *aesGzipCodec) CreateNewEncryptionKey() (types.PersistableKey, error) {
  const AES_256_KEY_LEN = 32
  null_key := types.PersistableKey{""}
  raw_key := make([]byte, AES_256_KEY_LEN)
  _, err := rand.Read(raw_key)
  if err != nil { util.Fatalf("Could not generate random key: %v", err) }
  secret := types.SecretKey{raw_key}
  persistable := self.encodeEncryptionKey(secret)
  fp := self.FingerprintKey(secret)

  if _,found := self.keyring[fp]; found {
    return null_key, fmt.Errorf("Fingerprint duplicated: '%s'", fp)
  }
  self.keyring[fp] = secret
  self.block, err = aes.NewCipher(self.keyring[fp].B)
  if err != nil { return null_key, err }
  self.cur_fp = fp
  return persistable, nil
}

func (self *aesGzipCodec) CurrentKeyFingerprint() types.PersistableString {
  return self.cur_fp
}

func (self *aesGzipCodec) FingerprintKey(key types.SecretKey) types.PersistableString {
  const fp_size = sha512.Size / 4
  if fp_size * 2 > len(key.B) {
    util.Fatalf("Fingerprinting a key that is too small.")
  }
  full_fp := sha512.Sum512(key.B)
  //return fmt.Sprintf("%x", raw_fp)
  raw_fp := base64.StdEncoding.EncodeToString(full_fp[:fp_size])
  return types.PersistableString{raw_fp}
}

func (self *aesGzipCodec) ReEncryptKeyring(pw_prompt func() ([]byte, error)) ([]types.PersistableKey, error) {
  persisted_keys := make([]types.PersistableKey, 0, len(self.keyring))
  var err error
  self.xor_key, err = derivatePassphrase(pw_prompt)
  if err != nil { return nil, err }

  // Put the current encryption key first.
  cur_key, found := self.keyring[self.CurrentKeyFingerprint()]
  if !found { return nil, fmt.Errorf("keyring invalid state.") }
  persisted_keys = append(persisted_keys, self.encodeEncryptionKey(cur_key))

  for fp,secret := range self.keyring {
    if fp.S == self.CurrentKeyFingerprint().S { continue }
    persisted_keys = append(persisted_keys, self.encodeEncryptionKey(secret))
  }
  return persisted_keys, nil
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

func (self *aesGzipCodec) getStreamDecrypter(key_fp types.PersistableString) (cipher.Block, cipher.Stream, error) {
  block := self.block
  if len(key_fp.S) > 0 {
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

func (self *aesGzipCodec) EncryptString(clear types.SecretString) types.PersistableString {
  null_first_block := make([]byte, self.block.BlockSize())
  padded_obfus := make([]byte, len(clear.S) + self.block.BlockSize())
  stream := self.getStreamEncrypter()
  stream.XORKeyStream(padded_obfus, null_first_block)
  stream.XORKeyStream(padded_obfus[self.block.BlockSize():], NoCopyStringToByteSlice(clear.S))
  return types.PersistableString{base64.StdEncoding.EncodeToString(padded_obfus)}
}

func (self *aesGzipCodec) DecryptString(key_fp types.PersistableString, obfus types.PersistableString) (types.SecretString, error) {
  null_str := types.SecretString{""}
  obfus_bytes, err_dec := base64.StdEncoding.DecodeString(obfus.S)
  if err_dec != nil { return null_str, err_dec }

  padded_plain := make([]byte, len(obfus_bytes))
  block, stream, err := self.getStreamDecrypter(key_fp)
  if err != nil { return null_str, err }
  if len(obfus_bytes) < self.block.BlockSize() {
    return null_str, fmt.Errorf("Obfuscated string is too short, expecting some larger than 1 block")
  }
  stream.XORKeyStream(padded_plain, obfus_bytes)
  plain := NoCopyByteSliceToString(padded_plain[block.BlockSize():])
  return types.SecretString{plain}, nil
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

func (self *aesGzipCodec) DecryptStream(ctx context.Context, key_fp types.PersistableString, input types.PipeReadEnd) (types.PipeReadEnd, error) {
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

