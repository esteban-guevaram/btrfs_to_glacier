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
  def get_by_ruuid(btrfs_path, ruuid):
    subvols = BtrfsSubvolList(btrfs_path)
    return next((n for n in subvols.subvols if n.ruuid == ruuid), None)

### END BtrfsSubvolList

