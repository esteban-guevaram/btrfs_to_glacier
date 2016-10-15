from common import *
from btrfs_session import *
from transaction_log import get_txlog
logger = logging.getLogger(__name__)

class BtrfsBackupOrchestrator :

  def __init__ (self, txlog_checker, btrfs_cmds):
    self.btrfs_cmds = btrfs_cmds
    self.txlog_checker = txlog_checker

  def snap_backup_clean_all_targets (self):
    self.txlog_checker.check_log_for_backup( get_txlog().iterate_through_records() )
    targets = get_conf().btrfs.target_subvols
    logger.info("Backup phase starting for targets : %r", targets)

    subvols = self.btrfs_cmds.find_subvols_from_paths(targets)
    predecessors = self.determine_predecessors_snap_for_delta(subvols)

    session = BackupSession.start_new()
    for subvol in subvols:
      self.snapshot_and_backup_to_file(session, subvol, predecessors[subvol.uuid])
    self.clean_old_backup_snaps(session, subvols)
    session.close()

    logger.info("Backup report: %s", session.print_summary())
    return session

  def snapshot_and_backup_to_file (self, session, subvol, predecessor):
    logger.info("Snapshot subvol, save snap to file, clean old snaps for subvol : %r", subvol)

    cur_snap = self.btrfs_cmds.create_snapshot(session, subvol)
    fileout = self.btrfs_cmds.send_volume(session, cur_snap, predecessor)

    assert cur_snap.uuid in get_txlog().recorded_snaps
    return fileout, cur_snap

  def clean_old_backup_snaps(self, session, subvols):
    sv_childs_in_txlog = self.btrfs_cmds.get_snaps_in_txlog_for_subvols(subvols)
    window = get_conf().btrfs.backup_clean_window
    assert window > 0

    for puuid,snaps in sv_childs_in_txlog.items():
      subvols_to_del = snaps[:-window]
      logger.info("These volumes are old and will be discarted : %r", subvols_to_del)

      for snap in subvols_to_del:
        assert snap.creation_utc <= snaps[-1].creation_utc, \
          'We expected the last snap to be the most recent one, it must not be deleted'
        self.btrfs_cmds.delete_snapshot(session, snap, puuid)

  def determine_predecessors_snap_for_delta(self, subvols):
    predecessors = { v.uuid : None for v in subvols }
    for record in get_txlog().reverse_iterate_through_records():
      if record.r_type == Record.NEW_SNAP and record.subvol.puuid in predecessors:
        predecessors[record.subvol.puuid] = record.subvol
        break

    logger.debug("Found the following last snapshots : %r", predecessors)
    return predecessors    

### END BtrfsBackupOrchestrator

