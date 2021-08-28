package types

import "context"
import "io"

// A version of `SecretKey` that has been encrypted.
// No secret can be deduced from a `PersistableKey` (unless the key-encrypting key is unknown).
// (PS: In go you cannot embed slice types.)
type PersistableKey struct { S string }
type SecretKey struct { B []byte }
type PersistableString struct { S string }
type SecretString struct { S string }

// Constant used when calling Decrypt functions, it signifies to use the current key for decryption.
var CurKeyFp = PersistableString{""}

// Encryption is a double edge sword.
// If a backup really needs to be done from scratch we need a way to retrieve the keys used to encrypt it.
// Ultimatelly only 2 passwords need to be remembered by a human :
// * The cloud provider login
// * The passphrase to decrypt the keys used to encrypt the backup data
// The encryption keys will be stored encrypted in the configuration and they will go to github.
type Codec interface {
  // Generates a random encryption key of a suitable length for the encryption algo.
  // The new key is stored in the keyring so that subsequent encrypts will use that key.
  // Returns a derived key that can be persisted.
  CreateNewEncryptionKey() (PersistableKey, error)
  // Generates a fingerprint for the key that can safely be stored in a non-secure place.
  // The key should be impossible to deduce from the fingerprint.
  // The fingerprint **must** be issue from a `SecretKey` so that it is not dependent on the method used to encrypt the keys.
  FingerprintKey(key SecretKey) PersistableString
  // Returns the fingerprint of the secret key used to encrypt.
  CurrentKeyFingerprint() PersistableString
  // Encrypts a textual string.
  EncryptString(clear SecretString) PersistableString
  // Re-encrypts all secret keys in the keyring with a new password.
  // Any new key added will use the new password.
  // The first key returned is the current encryption key used by the codec.
  ReEncryptKeyring(pw_prompt func() ([]byte, error)) ([]PersistableKey, error)
  // Decrypts a textual string. Does not provide a no-tamper guarantee.
  // `key_fp` may be left empty to use the current encryption key.
  DecryptString(key_fp PersistableString, obfus PersistableString) (SecretString, error)
  // Encrypts `input` and outputs obfuscated bytes to a new PipeReadEnd.
  // Takes ownership of `input` and will close it once done.
  EncryptStream(ctx context.Context, input PipeReadEnd) (PipeReadEnd, error)
  // Decrypts `input` and outputs plaintext bytes to a new PipeReadEnd.
  // `key_fp` may be left empty to use the current encryption key.
  // Takes ownership of `input` and will close it once done.
  DecryptStream(ctx context.Context, key_fp PersistableString, input PipeReadEnd) (PipeReadEnd, error)
}

// Does not encrypt, just forwards the input.
type MockCodec struct {
  Err error
  Fingerprint  PersistableString
  GenKeySecret SecretKey
  GenKeyPersistable PersistableKey
}

func (self *MockCodec) CreateNewEncryptionKey() (PersistableKey, error) {
  return self.GenKeyPersistable, self.Err
}

func (self *MockCodec) CurrentKeyFingerprint() PersistableString {
  return self.Fingerprint
}

func (self *MockCodec) FingerprintKey(key SecretKey) PersistableString {
  return self.Fingerprint
}

func (self *MockCodec) EncryptString(clear SecretString) PersistableString {
  return PersistableString{clear.S}
}

func (self *MockCodec) DecryptString(key_fp PersistableString, obfus PersistableString) (SecretString, error) {
  return SecretString{obfus.S}, self.Err
}

func (self *MockCodec) EncryptStream(ctx context.Context, input PipeReadEnd) (PipeReadEnd, error) {
  if self.Err != nil { return nil, self.Err }
  pipe := NewMockPipe()
  go func() {
    defer pipe.WriteEnd().Close()
    if ctx.Err() != nil { return }
    pipe.WriteEnd().PutErr(input.GetErr())
    io.Copy(pipe.WriteEnd(), input)
    pipe.WriteEnd().PutErr(input.GetErr())
  }()
  return pipe.ReadEnd(), nil
}

func (self *MockCodec) DecryptStream(ctx context.Context, key_fp PersistableString, input PipeReadEnd) (PipeReadEnd, error) {
  return self.EncryptStream(ctx, input)
}

func (self *MockCodec) ReEncryptKeyring(pw_prompt func() ([]byte, error)) ([]PersistableKey, error) {
  return []PersistableKey{self.GenKeyPersistable}, nil
}

