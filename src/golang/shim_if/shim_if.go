package shim_if

type SubvolumeInfoIf interface {
  Uuid() string
}

type Btrfsutil interface {
  SubvolumeInfo(path string, subvol_id int) (SubvolumeInfoIf, error)
}

