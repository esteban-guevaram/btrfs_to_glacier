package types

import "io"

type HasFileDescriptorIf interface { Fd() uintptr }
type CloseWithErrIf interface { CloseWithError(err error) error }

// Encapsulates both sides of a pipe.
type Pipe interface {
  ReadEnd() io.ReadCloser
  WriteEnd() io.WriteCloser
}

