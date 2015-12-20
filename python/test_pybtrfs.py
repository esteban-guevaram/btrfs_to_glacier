import unittest as ut
import sys, os, pybtrfs
from btrfs_subvol_list import *
from common import *
logger = logging.getLogger(__name__)

class TestPyBtrfs (ut.TestCase):

  def test_create_empty_node(self):
    node = pybtrfs.BtrfsNode()
    logger.debug( "empty node = '%r'", node )
    self.assertIsNone(node.name)
    self.assertIsNone(node.path)
    self.assertIsNone(node.uuid)
    self.assertIsNone(node.creation_utc)

  def test_create_subvol_tree(self):
    subvols = pybtrfs.build_subvol_list( get_conf().test.btrfs_path )
    logger.debug( "subvolume list = %r", subvols )
    self.assertTrue(len(subvols) > 0)
    self.assertTrue(all( n.name for n in subvols ))

  def test_check_for_snap_in_tree(self):
    subvols = pybtrfs.build_subvol_list( get_conf().test.btrfs_path )
    is_snap = dict( (n.name, n.is_snapshot()) for n in subvols )
    logger.debug( "snapshot dict = %r", is_snap )
    self.assertTrue(len(subvols) > 0)
    self.assertTrue(any( is_snap.values() ))

  def test_subvol_list_wrapper(self):
    subvols = BtrfsSubvolList( get_conf().test.btrfs_path )
    logger.debug( "subvolume list :\n%r", subvols )
    self.assertTrue(len(subvols.subvols) > 0)
    self.assertTrue(all( n.uuid for n in subvols.subvols ))

### END TestPyBtrfs

if __name__ == "__main__":
  ut.main()

