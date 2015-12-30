#include <python2.7/Python.h>
#include "pybtrfs_mod_function.h"
#include "pybtrfs_mod_type.h"

PyObject* StructMod;

PyMODINIT_FUNC initpybtrfs () {
  PyObject *mod = NULL;
  StructMod = NULL;

  FAIL_AND_GOTO_IF(clean_init,
    (StructMod = PyImport_ImportModule("struct")) == NULL);

  FAIL_AND_GOTO_IF(clean_init,
    (mod = Py_InitModule3("pybtrfs", module_methods, "salut")) == NULL );
  
  FAIL_AND_GOTO_IF(clean_init,
    PyType_BtrfsNodeType_Ready() < 0 );

  Py_INCREF(&BtrfsNodeType);

  FAIL_AND_GOTO_IF(clean_init,
    PyModule_AddObject(mod, "PICKLE_FORMAT", PyString_FromString(PICKLE_FORMAT)) < 0 );
  FAIL_AND_GOTO_IF(clean_init,
    PyModule_AddObject(mod, "BtrfsNode", (PyObject*)&BtrfsNodeType) < 0 );

  return;
  clean_init:
  Py_XDECREF(mod);
  Py_XDECREF(StructMod);
}

