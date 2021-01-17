package types

type Codec interface {
  IdToEncryptedUuid() error
  EncryptString() error
  EncryptStream() error
}

