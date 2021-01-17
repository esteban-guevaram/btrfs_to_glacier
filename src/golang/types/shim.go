package types

type SubvolumeInfoIf interface {
  Uuid() string
}

type Btrfsutil interface {
  CheckCompatibleWithHost() error
  SubvolumeInfo(path string, subvol_id int) (SubvolumeInfoIf, error)
}

