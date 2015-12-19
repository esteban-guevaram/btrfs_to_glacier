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

