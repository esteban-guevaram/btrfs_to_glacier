from common import *
from transaction_log import get_txlog, Record
from backup_file_commands import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class BtrfsCommands (object):

  def incremental_backup_all(self):
    backup_result = FsBackupResult()
    targets = get_conf().btrfs.target_subvols
    assert targets

    logger.info("These are the targets for backup : %r", targets)
    subvols = BtrfsSubvolList(targets[0])
    get_txlog().check_log_consistency(subvols)

    for target in targets:
      subvol = subvols.get_by_path(target)
      assert subvol
      fileout, snap = self.incremental_backup(subvol)
      backup_result.add_backup_subvol(subvol, snap, fileout)

    back_tx_log = BackupFileCommands.write_tx_log()
    backup_result.add_tx_log_save(back_tx_log)
    logger.info("Backup report: %s", backup_result.report())
    return backup_result

  def incremental_backup(self, subvol):
    logger.info("Creating backup file for %r", subvol)
    par_snap = self.determine_parent_snap_for_delta(subvol)
    cur_snap = self.create_snapshot(subvol)
    fileout = self.send_volume(cur_snap, par_snap)
    self.clean_old_snaps(subvol)

    subvols = BtrfsSubvolList(subvol.path)
    assert not par_snap or not subvols.get_by_uuid(par_snap.uuid)
    return fileout, cur_snap

  def clean_old_snaps(self, subvol):
    subvols = BtrfsSubvolList(subvol.path)
    childs = subvols.get_snap_childs(subvol)
    logger.info("These volumes are old and will be discarted : %r", childs[:-1])

    for child in childs[:-1]:
      # We expect the list to be sorted !!
      assert cmp(child.creation_utc, childs[-1].creation_utc) <= 0
      self.delete_subvol(child, subvol)

  def determine_parent_snap_for_delta(self, subvol):
    subvols = BtrfsSubvolList(subvol.path)
    last_snap_uuid = get_txlog().find_last_recorded_snap_uuid(subvol)
    if not last_snap_uuid:
      assert not subvols.get_snap_childs(subvol)
      return None
    else:
      last_snap = subvols.get_by_uuid(last_snap_uuid)
      assert last_snap
      return last_snap

  def delete_subvol (self, subvol, parent):
    assert parent.uuid == subvol.puuid
    assert subvol.is_snapshot() and subvol.is_readonly()
    call('btrfs subvolume delete -C ' + subvol.path)
    get_txlog().record_subvol_delete(subvol)
    assert not os.path.exists(subvol.path)

  def send_volume (self, current, parent=None):
    assert current.is_snapshot()
    fileout = '%s/backup_%s.btrfs' % (get_conf().btrfs.backup_subvol, current.name)
    assert not os.path.exists(fileout)

    if parent:
      assert parent.is_snapshot()
      assert parent.uuid != current.uuid and parent.puuid == current.puuid
      cmd = 'btrfs send -p %s %s' % (parent.path, current.path)
    else:  
      cmd = 'btrfs send %s' % current.path

    hashstr = BackupFileCommands.write_send_file(cmd, fileout)
    get_txlog().record_backup_file(fileout, hashstr, current, parent)
    logger.info("Wrote backup for %r at %s", current, fileout)
    return fileout

  def create_snapshot (self, source):
    full_dest = '%s/%s_%s' % (get_conf().btrfs.backup_subvol, source.name, timestamp.str)
    assert not os.path.exists(full_dest)

    call('btrfs subvolume snapshot -r %s %s' % (source.path, full_dest))
    assert os.path.exists(full_dest)

    subvols = BtrfsSubvolList(full_dest)
    new_vol = subvols.get_by_path(full_dest)
    assert new_vol
    logger.info("New snapshot from %r at %r", source, new_vol)
    get_txlog().record_snap_creation(source, new_vol)
    return new_vol


  def restore_all_subvols (self, txlog_name=None):
    restore_path = get_conf().btrfs.restore_path
    txlog = BackupFileCommands.fetch_tx_log(txlog_name)
    subvols = BtrfsSubvolList(restore_path)
    restore_result = FsRestoreResult()
    assert os.path.isdir(restore_path)

    for record in txlog.iterate_through_records():
      if record.r_type == Record.BACK_FILE:
        subvol = self.receive_volume(subvols, restore_path, record)
        restore_result.add_restored_snap(record, subvol)
    
    self.clean_old_restored_snaps(restore_result)
    logger.info("Restore report : %s", restore_result.report())
    return restore_result
  
  def receive_volume(self, subvols, restore_path, record):
    already_restored = subvols.get_by_ruuid(record.subvol.uuid)
    if already_restored:  
      logger.warn("Will not restore because there is already a matching subvolume : %r / %r", record, already_restored)
      return already_restored

    logger.debug("Restoring from %r", record)
    fileout = record.fileout
    BackupFileCommands.receive_subvol_file('btrfs receive ' + restore_path, fileout, record.hashstr)
    subvol = BtrfsSubvolList.find_by_ruuid(restore_path, record.subvol.uuid)
    assert subvol

    get_txlog().record_restore_snap(fileout, subvol)
    logger.info("Restored %r from %s", subvol, fileout)
    return subvol

  def clean_old_restored_snaps(self, restore_chain):
    pass

### BtrfsCommands

