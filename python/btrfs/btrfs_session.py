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
    return RestoreSession()

  def close (self):
    self.done = True

  # Handles both cases : delete snapshot and subvolume received from file
  def record_subvol_delete (self, subvol, ancestor_uuid=None):
    get_txlog().record_subvol_delete(subvol)
    key = ancestor_uuid or subvol.puuid
    if key not in self.deleted_subvols:
      self.deleted_subvols[key] = []
    self.deleted_subvols[key].append( subvol )
    self.child_subvols[key] = [ s for s in self.child_subvols[key] if s.uuid!=subvol.uuid ]

  def record_file_to_snap(self, fileout, subvol, ancestor_uuid):
    get_txlog().record_file_to_snap(fileout, subvol, ancestor_uuid)
    if ancestor_uuid not in self.child_subvols:
      self.child_subvols[ancestor_uuid] = []
      self.btrfs_sv_files[ancestor_uuid] = []
    self.child_subvols[ancestor_uuid].append( subvol )
    self.btrfs_sv_files[ancestor_uuid].append( fileout )

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
    return BackupSession()

  def record_snap_creation (self, subvol):
    get_txlog().record_snap_creation(subvol)
    key = subvol.puuid
    if key not in self.child_subvols:
      self.child_subvols[key] = []
    self.child_subvols[key].append( subvol )

  def record_snap_to_file(self, fileout, hashstr, current, pre_uuid):
    get_txlog().record_snap_to_file(fileout, hashstr, current, pre_uuid)
    key = current.puuid
    assert key in self.child_subvols
    if key not in self.btrfs_sv_files:
      self.btrfs_sv_files[key] = []
    self.btrfs_sv_files[key].append( fileout )

### END BackupSession

