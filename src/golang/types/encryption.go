package types

import "context"
import "io"

// IN go you cannot embed slice types
type PersistableKey struct { S string }
type PersistableString struct { S string }
type SecretString struct { S string }
type SecretKey struct { B []byte }

// Encryption is a double edge sword.
// If a backup really needs to be done from scratch we need a way to retrieve the keys used to encrypt it.
// Ultimatelly only 2 passwords need to be remembered by a human :
// * The cloud provider login
// * The passphrase to decrypt the keys used to encrypt the backup data
// The encryption keys will be stored encrypted in the configuration and they will go to github.
type Codec interface {
  // Generates a random encryption key of a suitable length for the encryption algo.
  // Then it derives a second key of the same length.
  // Only the second key should be persisted.
  GenerateEncryptionKey() (SecretKey, PersistableKey)
  // Generates a fingerprint for the key that can safely be stored in a non-secure place.
  // The key should be impossible to deduce from the fingerprint.
  FingerprintKey(key PersistableKey) string
  // Encrypts a textual string.
  EncryptString(clear SecretString) PersistableString
  // Decrypts a textual string. Does not provide a no-tamper guarantee.
  // `key_fp` may be left empty to use the current encryption key.
  DecryptString(key_fp string, obfus PersistableString) (SecretString, error)
  // Encrypts `input` and outputs obfuscated bytes to a new PipeReadEnd.
  // Takes ownership of `input` and will close it once done.
  EncryptStream(ctx context.Context, input PipeReadEnd) (PipeReadEnd, error)
  // Decrypts `input` and outputs plaintext bytes to a new PipeReadEnd.
  // `key_fp` may be left empty to use the current encryption key.
  // Takes ownership of `input` and will close it once done.
  DecryptStream(ctx context.Context, key_fp string, input PipeReadEnd) (PipeReadEnd, error)
}

// Does not encrypt, just forwards the input.
type MockCodec struct {
  Err error
  Fingerprint  string
  GenKeySecret SecretKey
  GenKeyPersistable PersistableKey
}

func (self *MockCodec) GenerateEncryptionKey() (SecretKey, PersistableKey) {
  return self.GenKeySecret, self.GenKeyPersistable
}

func (self *MockCodec) FingerprintKey(key PersistableKey) string {
  return self.Fingerprint
}

func (self *MockCodec) EncryptString(clear SecretString) PersistableString {
  return PersistableString{clear.S}
}

func (self *MockCodec) DecryptString(key_fp string, obfus PersistableString) (SecretString, error) {
  return SecretString{obfus.S}, self.Err
}

func (self *MockCodec) EncryptStream(ctx context.Context, input PipeReadEnd) (PipeReadEnd, error) {
  if self.Err != nil { return nil, self.Err }
  pipe := NewMockPipe()
  go func() {
    defer pipe.WriteEnd().Close()
    if ctx.Err() != nil { return }
    io.Copy(pipe.WriteEnd(), input)
  }()
  return pipe.ReadEnd(), nil
}

func (self *MockCodec) DecryptStream(ctx context.Context, key_fp string, input PipeReadEnd) (PipeReadEnd, error) {
  return self.EncryptStream(ctx, input)
}

