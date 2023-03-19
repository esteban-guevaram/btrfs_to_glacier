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
  "btrfs_to_glacier/workflow/restore_manager"
)

var ErrBadConfig = errors.New("bad_config_for_factory")

type Factory struct {
  Conf      *pb.Config
  Lu        types.Linuxutil
  Btrfsutil types.Btrfsutil
  Juggler   types.BtrfsPathJuggler
  VolAdmin  types.VolumeAdmin
}

func NewFactory(conf *pb.Config) (*Factory, error) {
  factory := &Factory{ Conf:conf, }
  var err error
  factory.Lu, err = shim.NewLinuxutil(conf)
  if err != nil { return nil, err }
  factory.Btrfsutil, err = shim.NewBtrfsutil(conf, factory.Lu)
  if err != nil { return nil, err }
  factory.Juggler, err = volume_source.NewBtrfsPathJuggler(
    conf, factory.Btrfsutil, factory.Lu)
  if err != nil { return nil, err }
  factory.VolAdmin, err = volume_source.NewVolumeAdmin(
    conf, factory.Btrfsutil, factory.Lu, factory.Juggler)
  return factory, err
}

func (self Factory) BuildCodec() (types.Codec, error) {
  if self.Conf.Encryption.Type == pb.Encryption_NOOP {
    return new(encryption.NoopCodec), nil
  }
  if self.Conf.Encryption.Type == pb.Encryption_AES {
    return encryption.NewCodec(self.Conf)
  }
  return nil, fmt.Errorf("%w bad encryption type", ErrBadConfig)
}

func (self *Factory) BuildBackupObjects(
    ctx context.Context, backup *pb.Backup) (types.AdminMetadata, types.AdminBackupContent, error) {
  var meta types.AdminMetadata
  var content types.AdminBackupContent

  codec, err := self.BuildCodec()
  if err != nil { return nil, nil, err }

  if backup.Type == pb.Backup_AWS {
    if backup.Aws == nil { return nil, nil, fmt.Errorf("%w missing aws config", ErrBadConfig) }
    aws_conf, err := encryption.NewAwsConfigFromTempCreds(ctx, self.Conf, pb.Aws_BACKUP_WRITER)
    if err != nil { return nil, nil, err }
    meta, err = aws_meta.NewMetadataAdmin(self.Conf, aws_conf, backup.Name)
    if err != nil { return nil, nil, err }
    content, err = aws_content.NewBackupContentAdmin(self.Conf, aws_conf, backup.Name, codec)
    if err != nil { return nil, nil, err }
  }
  if backup.Type == pb.Backup_FILESYSTEM {
    var err error
    if backup.Fs == nil { return nil, nil, fmt.Errorf("%w missing fs config", ErrBadConfig) }
    meta, err = local_fs.NewRoundRobinMetadataAdmin(self.Conf, self.Lu, backup.Name)
    if err != nil { return nil, nil, err }
    content, err = local_fs.NewRoundRobinContentAdmin(self.Conf, self.Lu, codec, backup.Name)
    if err != nil { return nil, nil, err }
  }
  if backup.Type == pb.Backup_MEM_EPHEMERAL {
    var err error
    meta, err = mem_only.NewMetadataAdmin(self.Conf)
    if err != nil { return nil, nil, err }
    content, err = mem_only.NewBackupContentAdmin(self.Conf, codec)
    if err != nil { return nil, nil, err }
  } else {
    return nil, nil, fmt.Errorf("%w bad backup type", ErrBadConfig)
  }
  return meta, content, nil
}

func (self *Factory) GetWorkflow(wf_name string) (types.ParsedWorkflow, error) {
  parsed_wf, err := util.WorkflowByName(self.Conf, wf_name)
  if err != nil { return parsed_wf, err }
  if (parsed_wf.Backup.Aws == nil) == (parsed_wf.Backup.Fs == nil) {
    return parsed_wf, fmt.Errorf("%w only one of aws or fs config needed", ErrBadConfig)
  }
  return parsed_wf, nil
}

func (self *Factory) BuildBackupManager(
    ctx context.Context, wf_name string) (types.DeferBackupManager, error) {
  res := types.DeferBackupManager{}
  wf, err := self.GetWorkflow(wf_name) 
  if err != nil { return res, err }
  meta, content, err := self.BuildBackupObjects(ctx, wf.Backup)
  if err != nil { return res, err }

  res.Create = func() (types.BackupManagerAdmin, error) {
    if err := meta.SetupMetadata(ctx); err != nil { return nil, err }
    if err := content.SetupBackupContent(ctx); err != nil { return nil, err }
    mgr, err := backup_manager.NewBackupManagerAdmin(self.Conf, meta, content, self.VolAdmin)
    return mgr, err
  }
  res.TearDown = func() error {
    if err := meta.TearDownMetadata(ctx); err != nil { return err }
    return content.TearDownBackupContent(ctx)
  }
  return res, nil
}

func (self *Factory) BuildRestoreManager(
    ctx context.Context, wf_name string) (types.DeferRestoreManager, error) {
  res := types.DeferRestoreManager{}
  wf, err := self.GetWorkflow(wf_name) 
  if err != nil { return res, err }
  meta, content, err := self.BuildBackupObjects(ctx, wf.Backup)
  if err != nil { return res, err }

  res.Create = func() (types.RestoreManager, error) {
    if err := meta.SetupMetadata(ctx); err != nil { return nil, err }
    if err := content.SetupBackupContent(ctx); err != nil { return nil, err }
    mgr, err := restore_manager.NewRestoreManager(self.Conf, wf.Restore.Name, meta, content, self.VolAdmin)
    return mgr, err
  }
  return res, nil
}

func BuildBackupRestoreCanary(conf *pb.Config) (types.BackupRestoreCanary, error) {
  return nil, nil
}

