#ifndef __PYBTRFS_MOD_FUNCTION_H__
#define __PYBTRFS_MOD_FUNCTION_H__
#include <python2.7/Python.h>
#include "btrfs_lib.h"

extern PyMethodDef module_methods[];
extern PyObject* StructMod;
extern const char* PICKLE_FORMAT;

PyObject* pybtrfs_build_btrfs_subvols_from_path(PyObject* self, PyObject* arg_tup);
PyObject* pybtrfs_pack_subvol_c_struct(PyObject* self, PyObject* arg_tup);
PyObject* pybtrfs_unpack_subvol_c_struct(PyObject* self, PyObject* arg_tup);

PyObject* pack_subvol_c_struct(PyObject* self);
PyObject* unpack_subvol_c_struct(PyObject* self, PyObject* arg_tup);

PyObject* build_py_node_list_from_c (struct root_lookup* result);

int build_py_node_and_add (struct root_info* tree, void* state);

#endif // __PYBTRFS_MOD_FUNCTION_H__

