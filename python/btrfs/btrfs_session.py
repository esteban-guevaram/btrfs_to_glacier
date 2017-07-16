from common import *
from transaction_log import get_txlog, Record
logger = logging.getLogger(__name__)

class RestoreSession (object):

  def __init__(self):
    self.child_subvols = {}
    self.deleted_subvols = {}
    self.btrfs_sv_files = {}
    self.done = False

  @staticmethod
  def start_new ():
    session = RestoreSession()
    get_txlog().record_restore_start()
    return session

  def close (self):
    get_txlog().record_restore_end()
    self.done = True

  # Handles both cases : delete snapshot and subvolume received from file
  def record_subvol_delete (self, subvol, ancestor_uuid=None):
    get_txlog().record_subvol_delete(subvol)
    key = ancestor_uuid or subvol.puuid
    self.deleted_subvols.setdefault(key, []).append( subvol )
    self.child_subvols[key] = [ s for s in self.child_subvols[key] if s.uuid!=subvol.uuid ]

  def record_file_to_snap(self, fileout, subvol, ancestor_uuid):
    get_txlog().record_file_to_snap(fileout, subvol, ancestor_uuid)
    self.child_subvols.setdefault(ancestor_uuid, []).append( subvol )
    self.btrfs_sv_files.setdefault(ancestor_uuid, []).append( fileout )

  def print_summary(self):
    lines = [ '' ]
    lines.append( " - Most recent snaps : %r" % [ l[-1] for l in self.child_subvols.values() ] )
    lines.append( " - Childs per puuid : %r"  % [ (binascii.hexlify(k),len(l)) for k,l in self.child_subvols.items() ] )
    lines.append( " - Deleted per puuid : %r" % [ (binascii.hexlify(k),len(l)) for k,l in self.deleted_subvols.items() ] )
    lines.append( "" )
    return '\n'.join(lines)  

### END RestoreSession

class BackupSession (RestoreSession):

  @staticmethod
  def start_new ():
    session = BackupSession()
    get_txlog().record_backup_start()
    return session

  def close (self):
    get_txlog().record_backup_end()
    self.done = True

  def record_snap_creation (self, subvol):
    get_txlog().record_snap_creation(subvol)
    key = subvol.puuid
    self.child_subvols.setdefault(key, []).append( subvol )

  def record_snap_to_file(self, fileout, hashstr, current, pre_uuid):
    get_txlog().record_snap_to_file(fileout, hashstr, current, pre_uuid)
    key = current.puuid
    assert key in self.child_subvols
    self.btrfs_sv_files.setdefault(key, []).append( fileout )

### END BackupSession

