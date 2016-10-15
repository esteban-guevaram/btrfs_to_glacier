import unittest as ut
import sys, os, pybtrfs, pickle, struct
from common import *
from routines_for_test import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class TestPyBtrfs (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    setup_filesystem(['-s'], get_conf().btrfs.target_subvols)
    pass

  def check_empty_subvol(self, subvol):
    self.assertIsNone(subvol.name)
    self.assertIsNone(subvol.path)
    self.assertIsNone(subvol.uuid)
    self.assertIsNone(subvol.puuid)
    self.assertIsNone(subvol.ruuid)
    self.assertIsNone(subvol.creation_utc)

  def check_subvol_equal(self, right, left):
    self.assertEqual(right.name,          left.name)
    self.assertEqual(right.path,          left.path)
    self.assertEqual(right.uuid,          left.uuid)
    self.assertEqual(right.puuid,         left.puuid)
    self.assertEqual(right.ruuid,         left.ruuid)
    self.assertEqual(right.creation_utc,  left.creation_utc)
    self.assertEqual(right.is_snapshot(),  left.is_snapshot())
    self.assertEqual(right.is_readonly(),  left.is_readonly())

  def test_raw_c_btrfs_code(self):
    root_fs = get_conf().test.root_fs
    call(['bin/btrfs_test', root_fs])

  def test_create_empty_node(self):
    node = pybtrfs.BtrfsNode()
    logger.debug( "empty node = '%r'", node )
    self.check_empty_subvol(node)

  def test_subvol_pickle_empty(self):
    node = pybtrfs.BtrfsNode()
    ser = pickle.dumps(node)
    clone = pickle.loads(ser)
    self.check_empty_subvol(clone)

  def test_create_subvol_tree(self):
    subvols = pybtrfs.build_subvol_list( get_conf().test.root_fs )
    logger.debug( "subvolume list = %r", subvols )
    self.assertTrue(len(subvols) > 0)
    self.assertTrue(all( n.name for n in subvols ))

  def test_check_for_snap_in_tree(self):
    subvols = pybtrfs.build_subvol_list( get_conf().test.root_fs )
    is_snap = dict( (n.path, n.is_snapshot()) for n in subvols )
    is_read = dict( (n.path, n.is_readonly()) for n in subvols )

    logger.debug( "snapshot dict = %r", is_snap )
    logger.debug( "readonly dict = %r", is_read )

    self.assertTrue(len(subvols) > 0)
    self.assertTrue(any( is_snap.values() ))
    self.assertTrue(any( is_read.values() ))

  def test_subvol_list_wrapper(self):
    subvols = BtrfsSubvolList.get_subvols_from_filesystem( get_conf().test.root_fs )
    logger.debug( "subvolume list :\n%r", subvols )
    self.assertTrue(len(subvols) > 0)
    self.assertTrue(all( n.uuid for n in subvols ))

  def test_subvol_pickle_with_data(self):
    subvols = pybtrfs.build_subvol_list( get_conf().test.root_fs )
    self.assertTrue(len(subvols) > 0)
    for subvol in subvols:
      ser = pickle.dumps(subvol)
      clone = pickle.loads(ser)
      self.check_subvol_equal(subvol, clone)

### END TestPyBtrfs

if __name__ == "__main__":
  conf_for_test()
  ut.main()

