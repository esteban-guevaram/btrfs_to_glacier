from common import *
from collections import namedtuple
from transaction_log import get_txlog
logger = logging.getLogger(__name__)

class BtrfsRestoreOrchestrator :

  def __init__ (self, txlog_checker, btrfs_cmds):
    self.btrfs_cmds = btrfs_cmds
    self.txlog_checker = txlog_checker

  def restore_subvols_from_received_files (self):
    self.txlog_checker.check_log_for_restore( get_txlog().iterate_through_records() )
    logger.info("Restore phase starting ...")

    puuid_to_restore_info = self.build_restore_info_from_txlog()
    session = RestoreSession.start_new()

    for puuid,info_list in puuid_to_restore_info.items():
      self.restore_all_and_clean_for_subvol(session, puuid, info_list)

    session.close()
    logger.info("Restore report : %s", session.print_summary())
    return restore_result


  def restore_all_and_clean_for_subvol (self, session, puuid, info_list):
    self.restore_path = get_conf().btrfs.restore_path
    assert os.path.isdir(self.restore_path)
    logger.info('For %r restoring %d receive files', puuid, len(info_list))

    for src_subvol,receive_filepath,hashstr in info_list:
      self.receive_volume(session, src_subvol, receive_filepath, restore_path)
      # We clean at the same time to avoid the filesystem to grow by keeping old snapshot data
      self.clean_snaps_outside_window(session, src_subvol)

  # { puuid : [ (subvol, receive_filepath, hashstr), ... ] }
  def build_restore_info_from_txlog (self):
    puuid_to_restore_info = {}
    snap_fileouts = set()
    fs_fileouts = set()

    for record in get_txlog().iterate_through_records():
      if record.r_type == Record.SNAP_TO_FILE:
        snap_fileouts.add(record.fileout)
        receive_filepath = os.path.join( get_conf().app.staging_dir, record.fileout )
        info = RestoreInfo(record.subvol, receive_filepath, record.hashstr)

        if record.subvol.puuid not in puuid_to_restore_info:
          puuid_to_restore_info[record.subvol.puuid] = [ info ]
        else:
          puuid_to_restore_info[record.subvol.puuid].append(info)  

      elif record.r_type == Record.FILESEG_START:
        fs_fileouts.add( record.fileout )

    # receive_filepath must be ordered for restore
    for info_list in puuid_to_restore_info.values():
      info_list.sort(key=lambda x:x.subvol.creation_utc)

    missing_fileouts = snap_fileouts.difference(fs_fileouts) 
    if missing_fileouts:
      logger.warn("Expecting the following files to be uploaded/downloaded in txlog : %r", missing_fileouts)
    return puuid_to_restore_info

  def clean_snaps_outside_window(self, session, src_subvol):
    window = get_conf().btrfs.restore_clean_window
    assert window > 0

    most_recent_sv = session.child_subvols[src_subvol.puuid][:-1]
    subvols_to_del = session.child_subvols[src_subvol.puuid][:-window]
    logger.info("These volumes are old and will be discarted : %r", subvols_to_del)
    
    for subvol in subvols_to_del:
      # We expect the list to be sorted !!
      assert subvol.creation_utc <= most_recent_sv.creation_utc
      self.btrfs_cmds.delete_received_subvol(session, subvol)

### END BtrfsRestoreOrchestrator

class RestoreInfo ( namedtuple('_RestoreInfo' 'subvol receive_filepath hashstr') ):
  pass

