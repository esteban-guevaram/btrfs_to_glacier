package types

import "io"

type HasFileDescriptorIf interface { Fd() uintptr }
type CloseWithErrIf interface { CloseWithError(err error) error }

type WriteEndIf interface {
  io.WriteCloser
  CloseWithErrIf
  // If error is nil then noop.
  // That way a previous error cannot be deleted.
  SetErr(error)
}
type ReadEndIf interface {
  io.ReadCloser
  GetErr() error
}

// Encapsulates both sides of a pipe.
type Pipe interface {
  ReadEnd()  ReadEndIf
  WriteEnd() WriteEndIf
}

