import unittest as ut
import sys, os

sys.path.append( os.path.realpath('./bin') )
import pybtrfs

class TestPyBtrfs (ut.TestCase):

  def test_create_empty_node(self):
    node = pybtrfs.BtrfsNode()
    self.assertIsNone(node.name)
    self.assertIsNone(node.path)
    self.assertIsNone(node.uuid)
    self.assertIsNone(node.creation_utc)
    print "empty node = '%r'" % node

  def test_create_subvol_tree(self):
    subvols = pybtrfs.build_subvol_list('/media/Aelia')
    self.assertTrue(len(subvols) > 0)
    self.assertTrue(all( n.name for n in subvols ))
    print "subvolume list = %r" % subvols

  def test_check_for_snap_in_tree(self):
    subvols = pybtrfs.build_subvol_list('/media/Aelia')
    is_snap = dict( (n.name, n.is_snapshot()) for n in subvols )
    self.assertTrue(len(subvols) > 0)
    self.assertTrue(any( is_snap.values() ))
    print "snapshot dict = %r" % is_snap

### END TestPyBtrfs

if __name__ == "__main__":
  ut.main()

