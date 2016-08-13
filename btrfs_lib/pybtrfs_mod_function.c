#include "pybtrfs_mod_function.h"
#include "pybtrfs_mod_type.h"

PyMethodDef module_methods[] = {
  {"build_subvol_list", pybtrfs_build_btrfs_subvols_from_path, METH_VARARGS, "Produces a list of all subvolumes found"},
  {NULL, NULL, 0, NULL}
};

PyObject* pybtrfs_build_btrfs_subvols_from_path(PyObject* self, PyObject* arg_tup) {
  const char *subvol = NULL;
  struct root_lookup result = {{NULL}};
  PyObject* node_list = NULL;

  FAIL_AND_GOTO_IF(build_subvol_list_clean,
    PyArg_ParseTuple(arg_tup, "s", &subvol) == 0);
  FAIL_AND_GOTO_IF(build_subvol_list_btrfs_err,
    build_btrfs_subvols_from_path (subvol, &result) != 0 );
  GOTO_IF_NULL(build_subvol_list_clean,
    node_list = build_py_node_list_from_c(&result));

  goto build_subvol_list_ok;

  build_subvol_list_btrfs_err:
  PyErr_SetString(PyExc_RuntimeError, "Failed build_btrfs_subvols_from_path");
  build_subvol_list_clean:
  Py_XDECREF(node_list);
  node_list = NULL;

  build_subvol_list_ok:
  free_subvol_rb_tree(&result);
  return node_list;
}

PyObject* build_py_node_list_from_c (struct root_lookup* result) {
  PyObject* node_list = NULL;
  
  GOTO_IF_NULL(build_py_node_list_from_c_clean,
    node_list = PyList_New(0) );

  FAIL_AND_GOTO_IF(build_py_node_list_from_c_err,
    visit_subvols_in_tree(result, build_py_node_and_add, node_list) != 0 );

  goto build_py_node_list_from_c_ok;

  build_py_node_list_from_c_err:
  PY_SET_RUNTIME_ERR("Unexpected error visiting the subvolume tree");

  build_py_node_list_from_c_clean:
  Py_XDECREF(node_list);
  node_list = NULL;

  build_py_node_list_from_c_ok:
  return node_list;
}

int build_py_node_and_add (struct root_info* subvol, void* state) {
  int ret = -1;
  PyObject *node_list = (PyObject*)state;
  PyObject *node = NULL;
  
  GOTO_IF_NULL(build_py_node_and_add_clean,
    node = build_from_root_info(subvol) );
  FAIL_AND_GOTO_IF(build_py_node_and_add_clean,
    (ret = PyList_Append(node_list, node)) != 0 );

  build_py_node_and_add_clean:
  Py_XDECREF(node);
  return ret;
}

const char* forge_pickle_format(int name_len, int path_len, int fpath_len) {
  static char buffer[NAME_MAX_LEN];
  int char_count = snprintf(buffer, NAME_MAX_LEN, 
    PICKLE_PREAMBLE FIXED_LEN_PICKLE "%ds%ds%ds",
    name_len, path_len, fpath_len);
  return char_count > 0? buffer : NULL;  
}

const char* extract_struct_header(PyObject* packed) {
  static char buffer[PREAMBLE_LEN + 1];
  const char* inner_str = NULL;

  GOTO_IF_NULL(extract_struct_header_fail,
    inner_str = PyBytes_AsString(packed));
  memcpy(buffer, inner_str, PREAMBLE_LEN);
  buffer[PREAMBLE_LEN] = '\0';

  return buffer;
  extract_struct_header_fail:
  return NULL;
}

const char* extract_preamble_and_build_pickle_format(PyObject* packed) {
  static char buffer[NAME_MAX_LEN];
  const char* packed_header = NULL;
  u8 name_len=0, path_len=0, fpath_len=0;
  PyObject *unpacked = NULL;

  #define UNPACK_TO_U8(field, tuple, index)  \
    field = PyLong_AsLong( PyTuple_GetItem(tuple, index) ); \
    FAIL_AND_GOTO_IF(extract_preamble_and_build_pickle_format_fail, PyErr_Occurred());

  GOTO_IF_NULL(extract_preamble_and_build_pickle_format_fail,
    packed_header = extract_struct_header(packed) );
  GOTO_IF_NULL(extract_preamble_and_build_pickle_format_fail,
    unpacked = PyObject_CallMethod(StructMod, "unpack", "sy#", PICKLE_PREAMBLE, packed_header, PREAMBLE_LEN) );

  UNPACK_TO_U8(name_len, unpacked, 0);
  UNPACK_TO_U8(path_len, unpacked, 1);
  UNPACK_TO_U8(fpath_len, unpacked, 2);

  int char_count = snprintf(buffer, NAME_MAX_LEN, 
    PADDED_PREAMBLE FIXED_LEN_PICKLE "%ds%ds%ds",
    name_len, path_len, fpath_len);

  Py_XDECREF(unpacked);
  return char_count > 0? buffer : NULL;  

  extract_preamble_and_build_pickle_format_fail:
  Py_XDECREF(unpacked);
  return NULL;
}

PyObject* pack_subvol_c_struct(PyObject* self) {
  struct BtrfsNode* subvol = (struct BtrfsNode*)self;
  u8 name_len = strnlen(subvol->node.name ? subvol->node.name : "", NAME_MAX_LEN); 
  u8 path_len = strnlen(subvol->node.path ? subvol->node.path : "", NAME_MAX_LEN); 
  u8 fpath_len = strnlen(subvol->node.full_path ? subvol->node.full_path : "", NAME_MAX_LEN); 

  PyObject* packed = PyObject_CallMethod(StructMod, "pack", "sBBBKKKKKKKKKiy#y#y#y#y#y#", 
    forge_pickle_format(name_len, path_len, fpath_len),
    name_len,
    path_len,
    fpath_len,
    subvol->node.root_id,
    subvol->node.root_offset,
    subvol->node.flags,
    subvol->node.ref_tree,
    subvol->node.dir_id,
    subvol->node.top_id,
    subvol->node.gen,
    subvol->node.ogen,
    (u64)subvol->node.otime,
    subvol->node.deleted,
    (char*)subvol->node.uuid, BTRFS_UUID_SIZE,
    (char*)subvol->node.puuid, BTRFS_UUID_SIZE,
    (char*)subvol->node.ruuid, BTRFS_UUID_SIZE,
    subvol->node.name ? subvol->node.name : "", name_len,
    subvol->node.path ? subvol->node.path : "", path_len,
    subvol->node.full_path ? subvol->node.full_path : "", fpath_len
  );
  return packed;
}

PyObject* unpack_subvol_c_struct(PyObject* self, PyObject* packed) {
  struct BtrfsNode* subvol = (struct BtrfsNode*)self;
  const char* pickle_format = NULL;
  PyObject *unpacked = NULL;

  GOTO_IF_NULL(unpack_subvol_c_struct_fail,
    pickle_format = extract_preamble_and_build_pickle_format(packed) );
  GOTO_IF_NULL(unpack_subvol_c_struct_fail,
    unpacked = PyObject_CallMethod(StructMod, "unpack", "sS", pickle_format, packed) );

  #define UNPACK_TO_U64(subvol, field, tuple, index)  \
    subvol->node.field = PyLong_AsUnsignedLongLongMask( PyTuple_GetItem(tuple, index) ); \
    FAIL_AND_GOTO_IF(unpack_subvol_c_struct_fail, PyErr_Occurred());

  #define UNPACK_TO_STR(subvol, field, tuple, index)  \
    const char* pasty(__,field) = PyBytes_AsString( PyTuple_GetItem(tuple, index) ); \
    if( strlen(pasty(__,field)) ) subvol->node.field = strdup( pasty(__,field) ); \
    FAIL_AND_GOTO_IF(unpack_subvol_c_struct_fail, PyErr_Occurred());

  #define UNPACK_TO_UUID(subvol, field, tuple, index)  \
    const char* pasty(__,field) = PyBytes_AsString( PyTuple_GetItem(tuple, index) ); \
    memcpy( subvol->node.field, pasty(__,field), BTRFS_UUID_SIZE); \
    FAIL_AND_GOTO_IF(unpack_subvol_c_struct_fail, PyErr_Occurred());
  
  UNPACK_TO_U64(subvol, root_id, unpacked, 0);
  UNPACK_TO_U64(subvol, root_offset, unpacked, 1);
  UNPACK_TO_U64(subvol, flags, unpacked, 2);
  UNPACK_TO_U64(subvol, ref_tree, unpacked, 3);
  UNPACK_TO_U64(subvol, dir_id, unpacked, 4);
  UNPACK_TO_U64(subvol, top_id, unpacked, 5);
  UNPACK_TO_U64(subvol, gen, unpacked, 6);
  UNPACK_TO_U64(subvol, ogen, unpacked, 7);
  UNPACK_TO_U64(subvol, otime, unpacked, 8);
  UNPACK_TO_U64(subvol, deleted, unpacked, 9);

  UNPACK_TO_UUID(subvol, uuid, unpacked, 10);
  UNPACK_TO_UUID(subvol, puuid, unpacked, 11);
  UNPACK_TO_UUID(subvol, ruuid, unpacked, 12);

  UNPACK_TO_STR(subvol, name, unpacked, 13);
  UNPACK_TO_STR(subvol, path, unpacked, 14);
  UNPACK_TO_STR(subvol, full_path, unpacked, 15);

  goto unpack_subvol_c_struct_ok;
  unpack_subvol_c_struct_fail:
  self = NULL;

  unpack_subvol_c_struct_ok:
  Py_XDECREF(unpacked);
  return self;
}

