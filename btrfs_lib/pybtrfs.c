#include <Python.h>
#include "pybtrfs_mod_function.h"
#include "pybtrfs_mod_type.h"

PyObject* StructMod;
static struct PyModuleDef module_def = {
   PyModuleDef_HEAD_INIT,
   "pybtrfs",  
   "Routines to scan for btrfs subvolumes",
   -1,      
   module_methods
};

PyMODINIT_FUNC PyInit_pybtrfs () {
  PyObject *mod = NULL;
  StructMod = NULL;

  GOTO_IF_NULL(clean_init,
    StructMod = PyImport_ImportModule("struct"));

  GOTO_IF_NULL(clean_init,
    mod = PyModule_Create(&module_def) );
  
  FAIL_AND_GOTO_IF(clean_init,
    PyType_BtrfsNodeType_Ready() < 0 );

  Py_INCREF(&BtrfsNodeType);

  FAIL_AND_GOTO_IF(clean_init,
    PyModule_AddObject(mod, "BtrfsNode", (PyObject*)&BtrfsNodeType) < 0 );

  return mod;
  clean_init:
  Py_XDECREF(mod);
  Py_XDECREF(StructMod);
  return NULL;
}

