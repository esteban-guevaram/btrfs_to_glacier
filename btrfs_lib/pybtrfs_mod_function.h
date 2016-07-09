#ifndef __PYBTRFS_MOD_FUNCTION_H__
#define __PYBTRFS_MOD_FUNCTION_H__
#include <python2.7/Python.h>
#include "btrfs_lib.h"

// First byte is left for future usage
#define PREAMBLE_LEN 4
#define PICKLE_PREAMBLE "<xBBB"
#define PADDED_PREAMBLE "<xxxx"
#define FIXED_LEN_PICKLE "QQQQQQQQQi" stringy(BTRFS_UUID_SIZE) "s" stringy(BTRFS_UUID_SIZE) "s" stringy(BTRFS_UUID_SIZE) "s"
#define NAME_MAX_LEN 256

extern PyMethodDef module_methods[];
extern PyObject* StructMod;

PyObject* pybtrfs_build_btrfs_subvols_from_path(PyObject* self, PyObject* arg_tup);

PyObject* pack_subvol_c_struct(PyObject* self);
PyObject* unpack_subvol_c_struct(PyObject* self, PyObject* arg_tup);

PyObject* build_py_node_list_from_c (struct root_lookup* result);

int build_py_node_and_add (struct root_info* tree, void* state);
const char* forge_pickle_format(int name_len, int path_len, int fpath_len);

#endif // __PYBTRFS_MOD_FUNCTION_H__

