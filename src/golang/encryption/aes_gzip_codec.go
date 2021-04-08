package encryption

import (
  "crypto/md5"
  "crypto/rand"
  "crypto/sha256"
  "encoding/base64"
  "fmt"
  "syscall"
  "unicode"
  "unicode/utf8"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "golang.org/x/crypto/ssh/terminal"
)

type aesGzipCodec struct {
  conf    *pb.Config
  keyring map[string]types.SecretKey
  xor_key  types.SecretKey
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

func (self *aesGzipCodec) EncryptString() error {
  return nil
}

func (self *aesGzipCodec) EncryptStream() error {
  return nil
}

