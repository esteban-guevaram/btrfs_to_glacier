package factory

import (
  "context"
  "errors"
  "fmt"

  "btrfs_to_glacier/encryption"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  aws_meta "btrfs_to_glacier/volume_store/aws_s3_metadata"
  aws_content "btrfs_to_glacier/volume_store/aws_s3_storage"
  "btrfs_to_glacier/volume_source"
  "btrfs_to_glacier/volume_store/local_fs"
  "btrfs_to_glacier/volume_store/mem_only"
  "btrfs_to_glacier/workflow/backup_manager"
)

var ErrBadConfig = errors.New("bad_config_for_factory")

func BuildCodec(conf *pb.Config) (types.Codec, error) {
  if conf.Encryption.Type == pb.Encryption_NOOP {
    return new(encryption.NoopCodec), nil
  }
  if conf.Encryption.Type == pb.Encryption_AES {
    return encryption.NewCodec(conf)
  }
  return nil, fmt.Errorf("%w bad encryption type", ErrBadConfig)
}

func BuildBackupManagerAdmin(
    ctx context.Context, conf *pb.Config, backup_name string) (types.BackupManagerAdmin, error) {
  var backup *pb.Backup
  if b,err := util.BackupByName(conf, backup_name); err == nil {
    if (b.Aws == nil) == (b.Fs == nil) {
      return nil, fmt.Errorf("%w only one of aws or fs config needed", ErrBadConfig)
    }
    backup = b
  } else {
    return nil, err
  }
  
  codec, err := BuildCodec(conf)
  if err != nil { return nil, err }
  lu, err := shim.NewLinuxutil(conf)
  if err != nil { return nil, err }
  btrfsutil, err := shim.NewBtrfsutil(conf, lu)
  if err != nil { return nil, err }
  juggler, err := volume_source.NewBtrfsPathJuggler(conf, btrfsutil, lu)
  if err != nil { return nil, err }
  vol_src, err := volume_source.NewVolumeSource(conf, btrfsutil, lu, juggler)
  if err != nil { return nil, err }

  var meta types.AdminMetadata
  var content types.AdminBackupContent

  if backup.Type == pb.Backup_AWS {
    if backup.Aws == nil { return nil, fmt.Errorf("%w missing aws config", ErrBadConfig) }
    aws_conf, err := encryption.NewAwsConfigFromTempCreds(ctx, conf, pb.Aws_BACKUP_WRITER)
    if err != nil { return nil, err }
    meta, err = aws_meta.NewMetadataAdmin(conf, aws_conf, backup_name)
    if err != nil { return nil, err }
    content, err = aws_content.NewBackupContentAdmin(conf, aws_conf, backup_name, codec)
    if err != nil { return nil, err }
  }
  if backup.Type == pb.Backup_FILESYSTEM {
    var err error
    if backup.Fs == nil { return nil, fmt.Errorf("%w missing fs config", ErrBadConfig) }
    meta, err = local_fs.NewRoundRobinMetadataAdmin(conf, lu, backup_name)
    if err != nil { return nil, err }
    content, err = local_fs.NewRoundRobinContentAdmin(conf, lu, codec, backup_name)
    if err != nil { return nil, err }
  }
  if backup.Type == pb.Backup_MEM_EPHEMERAL {
    var err error
    meta, err = mem_only.NewMetadataAdmin(conf)
    if err != nil { return nil, err }
    content, err = mem_only.NewBackupContentAdmin(conf, codec)
    if err != nil { return nil, err }
  } else {
    return nil, fmt.Errorf("%w bad backup type", ErrBadConfig)
  }
  mgr, err := backup_manager.NewBackupManagerAdmin(conf, meta, content, vol_src)
  return mgr, err
}

func BuildBackupManager(
    ctx context.Context, conf *pb.Config, backup_name string) (types.BackupManager, error) {
  return BuildBackupManagerAdmin(ctx, conf, backup_name)
}

func BuildRestoreManager(
    ctx context.Context, conf *pb.Config, restore_name string) (types.RestoreManager, error) {
  return nil, nil
}

func BuildBackupRestoreCanary(conf *pb.Config) (types.BackupRestoreCanary, error) {
  return nil, nil
}

