#include "pybtrfs_mod_function.h"
#include "pybtrfs_mod_type.h"

PyMethodDef module_methods[] = {
  {"build_subvol_list", pybtrfs_build_btrfs_subvols_from_path, METH_VARARGS, "Produces a list of all subvolumes found"},
  {"pack_subvol_c_struct", pybtrfs_pack_subvol_c_struct, METH_VARARGS, "Packs the contents of the btrfs root_info node into a string"},
  {"unpack_subvol_c_struct", pybtrfs_unpack_subvol_c_struct, METH_VARARGS, "Unpacks the contents of a string into a btrfs root_info"},
  {NULL, NULL, 0, NULL}
};

const char* PICKLE_FORMAT = "QQQQQQQQQ" stringy(BTRFS_UUID_SIZE) "s" stringy(BTRFS_UUID_SIZE) "s" stringy(BTRFS_UUID_SIZE) "s256s256s256si";

PyObject* pybtrfs_build_btrfs_subvols_from_path(PyObject* self, PyObject* arg_tup) {
  const char *subvol = NULL;
  struct root_lookup result = {{NULL}};
  PyObject* node_list = NULL;

  FAIL_AND_GOTO_IF(build_subvol_list_clean,
    PyArg_ParseTuple(arg_tup, "s", &subvol) == 0);
  FAIL_AND_GOTO_IF(build_subvol_list_btrfs_err,
    build_btrfs_subvols_from_path (subvol, &result) != 0 );
  FAIL_AND_GOTO_IF(build_subvol_list_clean,
    (node_list = build_py_node_list_from_c(&result)) == NULL );

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
  
  FAIL_AND_GOTO_IF(build_py_node_list_from_c_clean,
    (node_list = PyList_New(0)) == NULL );

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
  
  FAIL_AND_GOTO_IF(build_py_node_and_add_clean,
    (node = build_from_root_info(subvol)) == NULL);
  FAIL_AND_GOTO_IF(build_py_node_and_add_clean,
    (ret = PyList_Append(node_list, node)) != 0 );

  build_py_node_and_add_clean:
  Py_XDECREF(node);
  return ret;
}

PyObject* pybtrfs_pack_subvol_c_struct(PyObject* module, PyObject* arg_tup) {
  PyObject* subvol=NULL;
  PyArg_ParseTuple(arg_tup, "O", &subvol);
  return pack_subvol_c_struct(subvol);
}

PyObject* pack_subvol_c_struct(PyObject* self) {
  struct BtrfsNode* subvol = (struct BtrfsNode*)self;
  PyObject* packed = PyObject_CallMethod(StructMod, "pack", "sKKKKKKKKKs#s#s#sssi", PICKLE_FORMAT,
    subvol->node.root_id,
    subvol->node.root_offset,
    subvol->node.flags,
    subvol->node.ref_tree,
    subvol->node.dir_id,
    subvol->node.top_id,
    subvol->node.gen,
    subvol->node.ogen,
    (u64)subvol->node.otime,
    (char*)subvol->node.uuid, BTRFS_UUID_SIZE,
    (char*)subvol->node.puuid, BTRFS_UUID_SIZE,
    (char*)subvol->node.ruuid, BTRFS_UUID_SIZE,
    subvol->node.path ? subvol->node.path : "",
    subvol->node.name ? subvol->node.name : "",
    subvol->node.full_path ? subvol->node.full_path : "",
    subvol->node.deleted
  );
  return packed;
}

PyObject* pybtrfs_unpack_subvol_c_struct(PyObject* self, PyObject* arg_tup) {
  PyObject* subvol=NULL, *packed=NULL;
  PyArg_ParseTuple(arg_tup, "OS", &subvol, &packed);
  return unpack_subvol_c_struct(subvol, packed);
}

PyObject* unpack_subvol_c_struct(PyObject* self, PyObject* packed) {
  struct BtrfsNode* subvol = (struct BtrfsNode*)self;
  PyObject *unpacked = NULL;

  FAIL_AND_GOTO_IF(pybtrfs_unpack_subvol_c_struct_fail,
    (unpacked = PyObject_CallMethod(StructMod, "unpack", "sS", PICKLE_FORMAT, packed)) == NULL);

  #define UNPACK_TO_U64(subvol, field, tuple, index)  \
    subvol->node.field = PyInt_AsUnsignedLongLongMask( PyTuple_GetItem(tuple, index) ); \
    FAIL_AND_GOTO_IF(pybtrfs_unpack_subvol_c_struct_fail, PyErr_Occurred());

  #define UNPACK_TO_STR(subvol, field, tuple, index)  \
    const char* pasty(__,field) = PyString_AsString( PyTuple_GetItem(tuple, index) ); \
    if( strlen(pasty(__,field)) ) subvol->node.field = strdup( pasty(__,field) ); \
    FAIL_AND_GOTO_IF(pybtrfs_unpack_subvol_c_struct_fail, PyErr_Occurred());

  #define UNPACK_TO_UUID(subvol, field, tuple, index)  \
    const char* pasty(__,field) = PyString_AsString( PyTuple_GetItem(tuple, index) ); \
    memcpy( subvol->node.field, pasty(__,field), BTRFS_UUID_SIZE); \
    FAIL_AND_GOTO_IF(pybtrfs_unpack_subvol_c_struct_fail, PyErr_Occurred());
  
  UNPACK_TO_U64(subvol, root_id, unpacked, 0);
  UNPACK_TO_U64(subvol, root_offset, unpacked, 1);
  UNPACK_TO_U64(subvol, flags, unpacked, 2);
  UNPACK_TO_U64(subvol, ref_tree, unpacked, 3);
  UNPACK_TO_U64(subvol, dir_id, unpacked, 4);
  UNPACK_TO_U64(subvol, top_id, unpacked, 5);
  UNPACK_TO_U64(subvol, gen, unpacked, 6);
  UNPACK_TO_U64(subvol, ogen, unpacked, 7);
  UNPACK_TO_U64(subvol, otime, unpacked, 8);

  UNPACK_TO_UUID(subvol, uuid, unpacked, 9);
  UNPACK_TO_UUID(subvol, puuid, unpacked, 10);
  UNPACK_TO_UUID(subvol, ruuid, unpacked, 11);

  UNPACK_TO_STR(subvol, path, unpacked, 12);
  UNPACK_TO_STR(subvol, name, unpacked, 13);
  UNPACK_TO_STR(subvol, full_path, unpacked, 14);

  UNPACK_TO_U64(subvol, deleted, unpacked, 15);

  goto pybtrfs_unpack_subvol_c_struct_ok;
  pybtrfs_unpack_subvol_c_struct_fail:
  self = NULL;

  pybtrfs_unpack_subvol_c_struct_ok:
  Py_XDECREF(unpacked);
  return self;
}

