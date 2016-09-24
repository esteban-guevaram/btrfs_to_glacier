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

  def record_subvol_delete (self, subvol):
    get_txlog().record_file_to_snap(subvol)
    key = subvol.puuid
    if key not in self.deleted_subvols:
      self.deleted_subvols[key] = []
    self.deleted_subvols[key].append( subvol )
    del self.child_subvols[key]

  def record_file_to_snap(self, fileout, subvol):
    get_txlog().record_file_to_snap(fileout, subvol)
    key = subvol.puuid
    if key not in self.child_subvols:
      self.child_subvols[key] = []
      self.btrfs_sv_files[key] = []
    self.child_subvols[key].append( subvol )
    self.btrfs_sv_files[key].append( fileout )

  def print_summary(self):
    lines = [ '' ]
    lines.append( " - Most recent snaps : %r" % [ l[-1] for l in self.child_subvols.values() ] )
    lines.append( " - Childs per puuid : %r"  % [ (k,len(l)) for k,l in self.child_subvols.items() ] )
    lines.append( " - Deleted per puuid : %r" % [ (k,len(l)) for k,l in self.deleted_subvols.items() ] )
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

  def record_snap_to_file(self, fileout, hashstr, current, predecessor):
    get_txlog().record_snap_to_file(fileout, hashstr, current, predecessor)
    key = current.puuid
    if key not in self.btrfs_sv_files:
      self.btrfs_sv_files[key] = []
    self.btrfs_sv_files[key].append( fileout )
    assert key in self.child_subvols

### END BackupSession

