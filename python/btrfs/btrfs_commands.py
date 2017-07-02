from common import *
from transaction_log import get_txlog
logger = logging.getLogger(__name__)

class BtrfsCommands (object):

  def __init__ (self, native_btrfs, file_util):
    self.native_btrfs = native_btrfs
    self.file_util = file_util

  def find_subvols_from_paths (self, targets):
    if not targets: return []

    subvols = self.native_btrfs.get_subvols_from_filesystem(targets[0])
    filtered_sv = [ v for v in subvols if v.path in targets ]
    return filtered_sv
     
  def get_snaps_in_txlog_for_subvols (self, subvols):
    if not subvols: return {}
    sv_snaps = { v.uuid:[] for v in subvols }

    subvols = self.native_btrfs.get_subvols_from_filesystem( subvols[0].path )
    for subvol in subvols:
      if subvol.is_snapshot() and subvol.puuid in sv_snaps:
        if subvol.uuid in get_txlog().recorded_snaps:
          sv_snaps[subvol.puuid].append( subvol )

    logger.debug("For target subvolumes found snaps : %r", sv_snaps)
    return sv_snaps             

  def create_snapshot (self, session, source):
    full_dest = '%s/%s_%s' % (get_conf().btrfs.backup_subvol, source.name, timestamp.str)
    assert not os.path.exists(full_dest), 'snapshot dir exists : %s' % full_dest

    with tx_handler():
      call('btrfs subvolume snapshot -r %s %s' % (source.path, full_dest), as_root=True)
      subvols = self.native_btrfs.get_subvols_from_filesystem(full_dest)
      new_vol = subvols.get_by_path(full_dest)
      assert new_vol
      session.record_snap_creation(new_vol)

    logger.info("New snapshot from %r -> %r", source, new_vol)
    return new_vol

  def delete_snapshot (self, session, subvol, puuid):
    assert puuid == subvol.puuid
    assert subvol.is_snapshot() and subvol.is_readonly()
    assert subvol.uuid in get_txlog().recorded_snaps

    with tx_handler():
      call('btrfs subvolume delete -C ' + subvol.path, as_root=True)
      assert not os.path.exists(subvol.path)
      session.record_subvol_delete(subvol)

  def delete_received_subvol (self, session, subvol, ancestor_uuid):
    assert subvol.ruuid and subvol.is_readonly()
    assert subvol.uuid in get_txlog().recorded_restores

    with tx_handler():
      call('btrfs subvolume delete -C ' + subvol.path, as_root=True)
      assert not os.path.exists(subvol.path)
      session.record_subvol_delete(subvol, ancestor_uuid)

  def send_volume (self, session, current, predecessor=None):
    logger.debug("Send file from %r and %r", current, predecessor)

    assert current.is_snapshot()
    fileout = '%s/backup_%s.btrfs' % (get_conf().app.staging_dir, current.name)
    assert not os.path.exists(fileout)

    if predecessor:
      assert predecessor.is_snapshot()
      assert predecessor.uuid != current.uuid and predecessor.puuid == current.puuid
      cmd = 'btrfs send -p %s %s' % (predecessor.path, current.path)
      pre_uuid = predecessor.uuid
    else:  
      cmd = 'btrfs send %s' % current.path
      pre_uuid = None

    with tx_handler():
      hashstr = self.file_util.encrypt_from_cmd_stdout(cmd, fileout)
      session.record_snap_to_file(fileout, hashstr, current, pre_uuid)

    logger.info("Wrote backup for %r at %s", current, fileout)
    return fileout

  def receive_volume(self, session, src_subvol, receive_hashstr, receive_filepath, restore_path):
    already_restored = self.native_btrfs.find_by_ruuid(restore_path, src_subvol.uuid)
    if already_restored:  
      logger.warning("Not restoring, there is already a matching subvolume : %r / %r", src_subvol, already_restored)
      return already_restored

    with tx_handler():
      cmd = 'btrfs receive ' + restore_path
      hashstr = self.file_util.decrypt_file_into_cmd_stdin(cmd, receive_filepath)
      assert hashstr == receive_hashstr, "Restored volume from file but its contents are corrupt"

      subvol = self.native_btrfs.find_by_ruuid(restore_path, src_subvol.uuid)
      assert subvol
      session.record_file_to_snap(receive_filepath, subvol, src_subvol.puuid)

    logger.info("Restored %r -> %r", src_subvol, subvol)
    return subvol

### BtrfsCommands

