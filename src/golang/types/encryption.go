package types

import "context"

// IN go you cannot embed slice types
type PersistableKey struct { S string }
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
  EncryptString(clear string) []byte
  // Decrypts a textual string. Does not provide a no-tamper guarantee.
  // `key_fp` may be left empty to use the current encryption key.
  DecryptString(key_fp string, obfus []byte) (string, error)
  // Encrypts `input` and outputs obfuscated bytes to a new PipeReadEnd.
  // Takes ownership of `input` and will close it once done.
  EncryptStream(ctx context.Context, input PipeReadEnd) (PipeReadEnd, error)
  // Decrypts `input` and outputs plaintext bytes to a new PipeReadEnd.
  // `key_fp` may be left empty to use the current encryption key.
  // Takes ownership of `input` and will close it once done.
  DecryptStream(ctx context.Context, key_fp string, input PipeReadEnd) (PipeReadEnd, error)
}

