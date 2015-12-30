from common import *
from transaction_log import get_txlog
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class BtrfsCommands (object):

  def incremental_backup_all(self):
    targets = get_conf().btrfs.target_subvols
    subvols = BtrfsSubvolList(subvol.path)
    result_files = []
    logger.info("These are the targets for backup : %r", targets)

    for target in targets:
      subvol = subvols.get_by_path(target)
      assert subvol
      fileout, subvols = self.incremental_backup(subvol)
      result_files.append(fileout)

    logger.info("Wrote backup files : %r", result_files)
    return result_files  

  def incremental_backup(self, subvol):
    logger.info("Creating backup file for %r", subvol)
    par_snap = self.determine_parent_snap_for_delta(subvol)
    cur_snap = self.create_snapshot(subvol)
    fileout = self.send_volume(cur_snap, par_snap)
    subvols = self.clean_old_snaps(subvol)
    assert not subvols.get_by_uuid(par_snap.uuid)
    return fileout, subvols

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
    assert subvol.is_snapshot()
    sudo_call('btrfs subvolume delete -c ' + subvol.path, interactive=True)
    get_txlog().record_subvol_delete(subvol)
    assert not os.path.exists(subvol.path)
    return BtrfsSubvolList(parent.path)

  def send_volume (self, current, parent=None):
    assert current.is_snapshot()
    fileout = '%s/backup_%s_%s.btrfs' % (get_conf().btrfs.backup_subvol, current.name, timestamp.str)
    assert not os.path.exists(fileout)

    if parent:
      assert parent.is_snapshot()
      cmd = 'btrfs send -f %s -p %s %s' % (fileout, parent.path, current.path)
    else:  
      cmd = 'btrfs send -f %s %s' % (fileout, current.path)

    sudo_call(cmd)  
    assert os.path.exists(fileout)
    get_txlog().record_backup_file(fileout, current, parent)
    logger.info("Wrote backup for %r at %s", current, fileout)
    return fileout

  def create_snapshot (self, source):
    full_dest = '%s/%s_%s' % (get_conf().btrfs.backup_subvol, source.name, timestamp.str)
    assert not os.path.exists(full_dest)

    sudo_call('btrfs subvolume snapshot -r %s %s' % (source.path, full_dest), interactive=True)
    logger.info("New snapshot from %r at %s", source, full_dest)
    assert os.path.exists(full_dest)

    subvols = BtrfsSubvolList(full_dest)
    new_vol = subvols.get_by_path(full_dest)
    assert new_vol
    get_txlog().record_snap_creation(source, new_vol)
    return subvols

### BtrfsCommands

