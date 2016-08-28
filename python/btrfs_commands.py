from common import *
from transaction_log import get_txlog, Record
from file_utils import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class BtrfsCommands (object):

  def incremental_backup_all(self):
    backup_result = FsBackupResult()
    targets = get_conf().btrfs.target_subvols
    assert targets

    logger.info("These are the targets for backup : %r", targets)
    subvols = BtrfsSubvolList(targets[0])
    get_txlog().check_log_for_backup(subvols)

    for target in targets:
      subvol = subvols.get_by_path(target)
      assert subvol
      fileout, snap = self.incremental_backup(subvol)
      backup_result.add_backup_subvol(subvol, snap, fileout)

    back_tx_log = FileUtils.write_tx_log()
    backup_result.add_tx_log_save(back_tx_log)
    logger.info("Backup report: %s", backup_result.report())
    return backup_result

  def incremental_backup(self, subvol):
    logger.info("Creating backup file for %r", subvol)
    predecessor = self.determine_predecessor_snap_for_delta(subvol)
    cur_snap = self.create_snapshot(subvol)
    fileout = self.send_volume(cur_snap, predecessor)
    self.clean_old_backup_snaps(subvol)

    subvols = BtrfsSubvolList(subvol.path)
    assert cur_snap.uuid in get_txlog().recorded_snaps
    return fileout, cur_snap

  def clean_old_backup_snaps(self, subvol):
    subvols = BtrfsSubvolList(subvol.path)
    childs = [ s for s in subvols.get_snap_childs(subvol)
                 if s.uuid in get_txlog().recorded_snaps ]
    window = get_conf().btrfs.backup_clean_window
    assert window > 0
    subvols_to_del = childs[:-window]
    logger.info("These volumes are old and will be discarted : %r", subvols_to_del)

    for child in subvols_to_del:
      # We expect the list to be sorted !!
      assert child.creation_utc <= childs[-1].creation_utc
      self.delete_snapshot(child, subvol)
    return subvols_to_del  

  def determine_predecessor_snap_for_delta(self, subvol):
    subvols = BtrfsSubvolList(subvol.path)
    last_snap_uuid = get_txlog().find_last_recorded_snap_uuid(subvol)
    if not last_snap_uuid:
      snap_child_not_recorded = subvols.get_snap_childs(subvol)
      if snap_child_not_recorded:
        logger.warning("No snap predecessor in txlog, however some snaps exists for the subvol %r => %r",
          subvol, snap_child_not_recorded)
      return None
    else:
      last_snap = subvols.get_by_uuid(last_snap_uuid)
      assert last_snap
      return last_snap

  def delete_received_subvol (self, subvol, parent):
    assert not subvol.puuid or parent.uuid == subvol.puuid
    assert subvol.ruuid and subvol.is_readonly()
    assert subvol.uuid in get_txlog().recorded_restores
    call('btrfs subvolume delete -C ' + subvol.path)
    get_txlog().record_subvol_delete(subvol)
    assert not os.path.exists(subvol.path)

  def delete_snapshot (self, subvol, parent):
    assert parent.uuid == subvol.puuid
    assert subvol.is_snapshot() and subvol.is_readonly()
    assert subvol.uuid in get_txlog().recorded_snaps
    call('btrfs subvolume delete -C ' + subvol.path)
    get_txlog().record_subvol_delete(subvol)
    assert not os.path.exists(subvol.path)

  def send_volume (self, current, predecessor=None):
    assert current.is_snapshot()
    fileout = '%s/backup_%s.btrfs' % (get_conf().btrfs.send_file_staging, current.name)
    assert not os.path.exists(fileout)

    if predecessor:
      assert predecessor.is_snapshot()
      assert predecessor.uuid != current.uuid and predecessor.puuid == current.puuid
      cmd = 'btrfs send -p %s %s' % (predecessor.path, current.path)
    else:  
      cmd = 'btrfs send %s' % current.path

    hashstr = FileUtils.write_send_file(cmd, fileout)
    get_txlog().record_snap_to_file(fileout, hashstr, current, predecessor)
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


  def restore_all_subvols (self, archived_txlog=None):
    if archived_txlog:
      FileUtils.fetch_tx_log(archived_txlog)
    get_txlog().check_log_for_restore()

    restore_path = get_conf().btrfs.restore_path
    assert os.path.isdir(restore_path)
    subvols = BtrfsSubvolList(restore_path)
    restore_result = FsRestoreResult()

    for record in get_txlog().iterate_through_records():
      if record.r_type == Record.SNAP_TO_FILE:
        subvol = self.receive_volume(subvols, restore_path, record)
        restore_result.add_restored_snap(record, subvol)
    
    self.clean_old_restored_snaps(restore_result)
    logger.info("Restore report : %s", restore_result.report())
    return restore_result
  
  def receive_volume(self, subvols, restore_path, record):
    already_restored = subvols.get_by_ruuid(record.subvol.uuid)
    if already_restored:  
      logger.warning("Will not restore because there is already a matching subvolume : %r / %r", record, already_restored)
      return already_restored

    logger.debug("Restoring from %r", record)
    fileout = record.fileout
    FileUtils.receive_subvol_file('btrfs receive ' + restore_path, fileout, record.hashstr)
    subvol = BtrfsSubvolList.find_by_ruuid(restore_path, record.subvol.uuid)
    assert subvol

    get_txlog().record_file_to_snap(fileout, subvol)
    logger.info("Restored %r from %s", subvol, fileout)
    return subvol

  def clean_old_restored_snaps(self, restore_result):
    all_deleted = []
    for puuid,childs in restore_result.child_subvols.items():
      previous_vol = None
      window = get_conf().btrfs.restore_clean_window
      assert window > 0
      subvols_to_del = childs[:-window]
      logger.info("These volumes are old and will be discarted : %r", subvols_to_del)
      
      for subvol in subvols_to_del:
        # We expect the list to be sorted !!
        assert subvol.creation_utc <= restore_result.last_childs[puuid].creation_utc
        self.delete_received_subvol(subvol, previous_vol)
        previous_vol = subvol

      all_deleted.extend(subvols_to_del)
    return all_deleted      

### BtrfsCommands

