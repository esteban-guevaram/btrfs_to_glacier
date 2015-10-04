from common import *
from btrfs_data import *
import copy
logger = logging.getLogger(__name__)

class BtrfsTreeBuilder (object):
  
  def __init__(self):
    pass

  def build_root (self, device):
    cmd_out = sudo('btrfs filesystem show ' + device)
    sc_label = re.search(r"label\s*:\s*'?(\w+)'?",    cmd_out, re.IGNORECASE)
    sc_uuid =  re.search(r"uuid\s*:\s*([0-9a-f\-]+)", cmd_out, re.IGNORECASE)
    assert sc_label and sc_uuid, "Bad btrfs command output"

    root = BtrfsRoot(label=sc_label.group(1), uuid=sc_uuid.group(1))
    cmd_out = sudo('blkid -o device -t UUID=' + root.uuid)
    root.devices = [ d.strip() for d in cmd_out.split('\n') if d.strip() ]
    
    logger.debug('Found root : %r', root)
    return root

  def get_filesystem_mounts (self, root):
    mounts = {}
    cmd_out = call('findmnt --all --canonicalize --pairs --output SOURCE,TARGET --source UUID=' + root.uuid)
    rx_mount = re.compile(r'target\s*=\s*"([^"]+)"',          re.IGNORECASE)
    rx_label = re.compile(r'source\s*=\s*".*\[([^]]+)\].*"',  re.IGNORECASE)

    for line in cmd_out.split('\n'):
      if not line.strip(): continue
      sc_mount = rx_mount.search(line)
      sc_label = rx_label.search(line)
      assert sc_mount and sc_label, "%r / %r" % (sc_mount, sc_label)
      label = tuple( p for p in sc_label.group(1).split('/') if p )
      mounts[label] = sc_mount.group(1)

    assert mounts
    logger.debug('Found mounts : %r', mounts)
    return mounts

  def build_node_list_from_cmd (self, cmd_out):
    nodes = {}
    rx_id       = re.compile(r'\bid\s+(\d+)',                   re.IGNORECASE)
    rx_par      = re.compile(r'\bparent\s+(\d+)',               re.IGNORECASE)
    rx_uuid     = re.compile(r'\buuid\s+([0-9a-f\-]+)',         re.IGNORECASE)
    rx_par_uuid = re.compile(r'\bparent_uuid\s+([0-9a-f\-]+)',  re.IGNORECASE)
    rx_path     = re.compile(r'\bpath\s+(\w+)',                 re.IGNORECASE)

    for line in cmd_out.split('\n'):
      if not line.strip(): continue
      sc_id       = rx_id.search(line)
      sc_par      = rx_par.search(line)
      sc_uuid     = rx_uuid.search(line)
      sc_par_uuid = rx_par_uuid.search(line)
      sc_path     = rx_path.search(line)

      assert sc_uuid 
      node = BtrfsNode(uuid=sc_uuid.group(1))
      if sc_par:      node.par_id   = int(sc_par.group(1))
      if sc_id:       node.id       = int(sc_id.group(1))
      if sc_path:     node.label    = sc_path.group(1)
      if sc_par_uuid:
        if sc_par_uuid.group(1) == '-': 
          node.par_uuid = None
        else:
          node.par_uuid = sc_par_uuid.group(1)
      nodes[node.uuid] = node

    logger.debug('Nodes found : %r', nodes)
    return nodes

  def attach_nodes (self, parent, uuid, nodes):
    if not nodes: return  
    if not hasattr(parent, 'childs'):
      parent.childs = []

    for key in list( nodes.keys() ):
      if key not in nodes: continue
      node = nodes[key]
      if node.par_uuid == uuid:
        parent.childs.append( node )
        node.parent = parent
        del nodes[key]
        self.attach_nodes(node, node.uuid, nodes)

    #logger.debug('For %r found childs : %r', uuid, parent.childs)
    return parent    

  def add_subvolume_nodes (self, root, mounts):
    path = mounts.values()[0]
    cmd_out = sudo('btrfs subvolume list -pqu ' + path)
    nodes = self.build_node_list_from_cmd(cmd_out)
    
    tmp_nodes = copy.copy(nodes)
    self.attach_nodes(root, None, tmp_nodes)
    assert root.childs and not tmp_nodes
    return nodes

  def determine_node_mount_path (self, tree, mounts):
    def visitor (node):
      if node.uuid in mounts:
        node.mount = mounts[node.uuid]
      else:
        node.mount = node.parent.mount + '/' + node.label
      logger.debug('mount=%s', node.mount)
      
    tree.depth_first_visit(visitor)

  def build_fs_tree (self, device):
    root = self.build_root(device)
    mounts = self.get_filesystem_mounts (root)
    nodes = self.add_subvolume_nodes (root, mounts)
    tree = BtrfsTree(root)

    #self.determine_node_mount_path (tree, mounts)
    #self.determine_snapshots_nodes (nodes)
    #self.determine_snapshot_parents (nodes)

    logger.info('Built tree :\n%r', tree)
    return tree

### BtrfsTreeBuilder

class BtrfsTree (object): 
  
  def __init__ (self, root):
    self.root = root

  def depth_first_visit (self, visitor, parent=None, visit_root=False):
    if not parent:
      if visit_root: self.root.accept(visitor)
      self.depth_first_visit(visitor, self.root)
    else:  
      for n in parent.childs: 
        n.accept(visitor)
        self.depth_first_visit(visitor, n)

  def print_childs (self, depth, node):
    lines = [ '  ' * depth + repr(node) ]
    for n in node.childs: 
      lines.extend( self.print_childs(depth+1, n) )
    return lines  

  def __repr__ (self):
    lines = self.print_childs(0, self.root)
    return '\n'.join( lines )


### BtrfsTree

BtrfsTreeBuilder().build_fs_tree('/dev/sdb1')

