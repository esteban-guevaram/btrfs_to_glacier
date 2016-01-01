import logging
import pybtrfs
logger = logging.getLogger(__name__)

class BtrfsSubvolList (object):

  def __init__(self, btrfs_path):
    self.subvols = pybtrfs.build_subvol_list(btrfs_path)
    assert self.subvols

  def get_by_ruuid(self, ruuid):
    return next((n for n in self.subvols if n.ruuid == ruuid), None)

  def get_by_uuid(self, uuid):
    return next((n for n in self.subvols if n.uuid == uuid), None)

  def get_by_path(self, path):
    return next((n for n in self.subvols if n.path == path), None)

  def get_snap_childs(self, subvol):
    snaps = [ n for n in self.subvols if n.is_snapshot() and n.puuid == subvol.uuid ]
    snaps = sorted(snaps, key=(lambda x: x.creation_utc))
    return snaps

  def __repr__ (self):
    return "\n".join( repr(n) for n in self.subvols )


  @staticmethod
  def find_by_ruuid(btrfs_path, ruuid):
    subvols = BtrfsSubvolList(btrfs_path)
    return next((n for n in subvols.subvols if n.ruuid == ruuid), None)

### END BtrfsSubvolList

class FsRestoreResult (object):

  def __init__(self):
    self.child_subvols = {}
    self.restored_files = {}
    self.last_childs = {}

  def add_restored_snap(self, record, subvol):
    key = record.subvol.puuid
    if key not in self.child_subvols:
      self.child_subvols[key] = []
      self.restored_files[key] = []
    self.child_subvols[key].append( subvol )
    self.restored_files[key].append( record.fileout )
    self.last_childs[key] = subvol

  def report(self):
    lines = [ '', '#'*40 + ' RESTORE REPORT ' + '#'*40, '' ]
    lines.append( " - Last snaps : %r" % list(self.last_childs.values()) )
    lines.append( " - Files restored : %r" % list(self.restored_files.values()) )
    lines.append( "\nChilds per original subvolume" )
    for k,v in self.child_subvols.items():
      lines.append( " - %r : %r" % (k, [ s.name for s in v ]) )
    lines.append( "" )
    return '\n'.join(lines)  

### END FsRestoreResult

class FsBackupResult (object):

  def __init__(self):
    self.back_tx_log = None
    self.restored_files = {}
    self.child_subvols = {}

  def add_backup_subvol(self, subvol, snap, fileout):
    key = (subvol.uuid, subvol.name)
    assert key not in self.child_subvols and key not in self.restored_files
    self.child_subvols[key] = snap
    self.restored_files[key] = fileout

  def add_tx_log_save(self, back_tx_log):
    self.back_tx_log = back_tx_log

  def report(self):
    lines = [ '', '#'*40 + ' BACKUP REPORT ' + '#'*40, '' ]
    lines.append( " - Tx log backup : %r" % self.back_tx_log )
    lines.append( " - Send files : %r" % list(self.restored_files.values()) )
    lines.append( "\nSnapshots created :" )
    for k,v in self.child_subvols.items():
      lines.append( " - %r : %r" % (k, v) )
    lines.append( "" )
    return '\n'.join(lines)  

### END FsBackupResult

