from common import *
from btrfs_data import *
import copy, weakref
logger = logging.getLogger(__name__)

class BtrfsTreeBuilder (object):
  
  def build_root (self, device):
    cmd_out = sudo_call('btrfs filesystem show ' + device, get_conf().app.interactive)
    sc_label = re.search(r"label\s*:\s*'?(\w+)'?",    cmd_out, re.IGNORECASE)
    sc_uuid =  re.search(r"uuid\s*:\s*([0-9a-f\-]+)", cmd_out, re.IGNORECASE)
    assert sc_label and sc_uuid, "Bad btrfs command output"

    root = BtrfsRoot(label=sc_label.group(1), uuid=sc_uuid.group(1))
    cmd_out = sudo_call('blkid -o device -t UUID=' + root.uuid, get_conf().app.interactive)
    root.devices = [ d.strip() for d in cmd_out.split('\n') if d.strip() ]
    
    logger.debug('Found root : %r', root)
    return root

  def get_filesystem_mounts (self, root):
    mounts = []
    cmd_out = call('findmnt --all --canonicalize --pairs --fstab --output TARGET --source UUID=' + root.uuid)
    rx_mount = re.compile(r'target\s*=\s*"([^"]+)"', re.IGNORECASE)

    for line in cmd_out.split('\n'):
      if not line.strip(): continue
      sc_mount = rx_mount.search(line)
      assert sc_mount 
      mounts.append( sc_mount.group(1) )

    assert mounts
    logger.debug('Found mounts : %r', mounts)
    return mounts

  def build_node_from_mount_path (self, mount):
    l1 = lambda sc: int(sc.group(1))
    l2 = lambda sc: sc.group(1)
    l3 = lambda sc: (sc.group(1) != '-' and sc.group(1)) or None
    l4 = lambda sc: datetime.datetime.strptime(sc.group(1), '%Y-%m-%d %H:%M:%S')

    extract_spec = {  
      'id'        : ( re.compile(r'^\s+object id:\s+(\d+)',            re.IGNORECASE), l1 ),
      'par_id'    : ( re.compile(r'^\s+parent:\s+(\d+)',               re.IGNORECASE), l1 ),
      'top_id'    : ( re.compile(r'^\s+top level:\s+(\d+)',            re.IGNORECASE), l1 ),
      'uuid'      : ( re.compile(r'^\s+uuid:\s+([0-9a-f\-]+)',         re.IGNORECASE), l2 ),
      'par_uuid'  : ( re.compile(r'^\s+parent uuid:\s+([0-9a-f\-]+)',  re.IGNORECASE), l3 ),
      'label'     : ( re.compile(r'^\s+name:\s+(\w+)',                 re.IGNORECASE), l2 ),
      'otime'     : ( re.compile(r'^\s+creation time:\s+(\d+-\d+-\d+\s+\d+:\d+:\d+)', re.IGNORECASE), l4 ),
    }

    cmd_out = sudo_call('btrfs subvolume show ' + mount, get_conf().app.interactive)
    node = self.build_from_extract_spec(cmd_out.split('\n'), extract_spec)
    node.mount = mount
    return node

  def build_from_extract_spec (self, cmd_lines, extract_spec):
    build_args = {}
    for line in cmd_lines:
      for field,tup in extract_spec.items():
        sc = tup[0].search(line)
        if sc:
          build_args[field] = tup[1](sc)

    node = BtrfsNode(**build_args)
    logger.debug('Built node : %r', node)
    return node

  def build_nodes_from_mount_paths (self, mounts):
    nodes = {}
    for mount in mounts:
      try:
        node = self.build_node_from_mount_path(mount)
        nodes[node.uuid] = node
      except:
        logger.info('Ignoring mount path %r', mount)
    return nodes
  
  def build_nodes_from_sv_list (self, mount, flags):
    nodes = {}
    l1 = lambda sc: int(sc.group(1))
    l2 = lambda sc: sc.group(1)
    l3 = lambda sc: (sc.group(1) != '-' and sc.group(1)) or None
    l4 = lambda sc: os.path.basename( sc.group(1) )

    extract_spec = {  
      'id'        : ( re.compile(r'\bid\s+(\d+)',                   re.IGNORECASE), l1 ),
      'par_id'    : ( re.compile(r'\bparent\s+(\d+)',               re.IGNORECASE), l1 ),
      'top_id'    : ( re.compile(r'\btop level\s+(\d+)',            re.IGNORECASE), l1 ),
      'uuid'      : ( re.compile(r'\buuid\s+([0-9a-f\-]+)',         re.IGNORECASE), l2 ),
      'par_uuid'  : ( re.compile(r'\bparent_uuid\s+([0-9a-f\-]+)',  re.IGNORECASE), l3 ),
      'mount'     : ( re.compile(r'\bpath\s+(.+)',                  re.IGNORECASE), l4 ),
    }

    cmd_out = sudo_call('btrfs subvolume list -%s %s' % (flags, mount), get_conf().app.interactive)

    for line in cmd_out.split('\n'):
      if not line.strip(): continue
      node = self.build_from_extract_spec([line], extract_spec)
      nodes[node.uuid] = node

    logger.debug('Nodes found : %r', nodes)
    return nodes

  def attach_nodes (self, parent, uuid, nodes):
    if not nodes: return  

    for key in list( nodes.keys() ):
      if key not in nodes: continue
      node = nodes[key]
      if node.par_uuid == uuid:
        parent.childs.append( node )
        node.get_parent = weakref.ref(parent)
        del nodes[key]
        self.attach_nodes(node, node.uuid, nodes)

    #logger.debug('For %r found childs : %r', uuid, parent.childs)
    return parent    

  def attach_single_node (self, tree, node):
    for parent in tree.depth_first_it():
      if parent.uuid == node.par_uuid:
        parent.childs.append( node )
        node.get_parent = weakref.ref(parent)
    assert node.get_parent()    

  def determine_node_mount_path (self, nodes):
    id_to_nodes = { n.id : n for n in nodes }
    count, previous = 0, -1

    while count > previous:
      previous = count
      for node in nodes:
        if os.path.isabs( node.mount ): continue
        par_path = id_to_nodes[node.top_id].mount
        if os.path.isabs( par_path ):
          node.mount = par_path + '/' + node.mount
          count += 1
    
    assert all( os.path.isdir(n.mount) and os.path.isabs(n.mount) for n in nodes )
    return nodes

  def complete_missing_node_attributes (self, nodes):
    for node in nodes:
      if not node.label or not node.otime:
        same_node = self.build_node_from_mount_path(node.mount)
        node.otime = same_node.otime
        node.label = same_node.label
    assert all( n.label and n.otime for n in nodes )
    return nodes    

  def determine_snapshots_nodes (self, fs_path, nodes):
    id_to_snaps = { n.id : n for n in self.build_nodes_from_sv_list(fs_path, 'rsu').values() }
    for node in nodes:
      if node.id in id_to_snaps:
        node.type = BtrfsNode.SNAP
      else:  
        node.type = BtrfsNode.SUBVOL
    assert len([ n for n in nodes if n.type == BtrfsNode.SNAP ]) == len(id_to_snaps)
    return node    

  def build_all_child_nodes (self, root, mounts):
    nodes = self.build_nodes_from_mount_paths(mounts)
    second_nodes = self.build_nodes_from_sv_list(mounts[0], 'pqu')

    for k in second_nodes.keys():
      if k not in nodes:
        nodes[k] = second_nodes[k]
    
    node_list = nodes.values()
    self.determine_node_mount_path (node_list)
    self.complete_missing_node_attributes (node_list)
    self.determine_snapshots_nodes (mounts[0], node_list)
    return nodes

  def build_fs_tree (self, device):
    root = self.build_root(device)
    mounts = self.get_filesystem_mounts (root)
    nodes = self.build_all_child_nodes (root, mounts)

    tmp_nodes = copy.copy(nodes)
    self.attach_nodes(root, None, tmp_nodes)
    assert root.childs and not tmp_nodes

    tree = BtrfsTree(root)
    logger.info('Built tree :\n%r', tree)
    return tree

  def remove_node (self, node):
    #sudo_call('btrfs subvolume delete -c ' + node.mount, interactive=True)
    node.get_parent().childs.remove(node)
    node.get_parent = None
    return node

  def add_node (self, tree, fs_path, n_type):
    node = self.build_node_from_mount_path(fs_path, 'pqu')
    node.type = n_type
    node.mount = fs_path
    self.attach_single_node(tree, node)
    return node

  def send_volume (self, n_source, n_parent=None):
    if n_parent:
      cmd = 'btrfs send -p %s %s' % (n_parent.mount, n_source.mount)
    else:  
      cmd = 'btrfs send ' + n_source.mount

  def create_snapshot (self, tree, n_source, n_dest):
    full_dest = '%s/%s_%s' % (n_dest.mount, n_source.label, timestamp.str)
    assert not os.path.exists(full_dest)

    sudo_call('btrfs subvolume snapshot -r %s %s' % (n_source.mount, full_dest), interactive=True)
    return self.add_node(tree, full_dest, BtrfsNode.SNAP)

### BtrfsTreeBuilder

class BtrfsTree (object): 
  
  def __init__ (self, root):
    self.root = root

  def depth_first_it (self, parent=None, visit_root=True):
    if not parent:
      if visit_root: yield self.root
      self.depth_first_it(self.root)
    else:  
      for n in parent.childs: 
        yield n
        self.depth_first_it(n)

  def print_childs (self, depth, node):
    lines = [ '  ' * depth + repr(node) ]
    for n in node.childs: 
      lines.extend( self.print_childs(depth+1, n) )
    return lines  

  def __repr__ (self):
    lines = self.print_childs(0, self.root)
    return '\n'.join( lines )

### BtrfsTree

