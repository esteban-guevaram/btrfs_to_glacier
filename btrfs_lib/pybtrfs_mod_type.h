#ifndef __PYBTRFS_MOD_TYPE_H__
#define __PYBTRFS_MOD_TYPE_H__
#include <python2.7/Python.h>
#include "btrfs_lib.h"

#define stringy_helper(arg) #arg
#define stringy(arg) stringy_helper(arg)

#define pasty_helper(a,b) a ## b
#define pasty(a,b) pasty_helper(a,b)

#define FAIL_AND_GOTO_IF(label, exp)       \
  if (exp) goto label; 

#define PY_SET_RUNTIME_ERR(msg)                         \
  if (!PyErr_Occurred())                                \
    PyErr_SetString(PyExc_RuntimeError,                 \
      __FILE__ ":" stringy(__LINE__) " " msg);

#define PYTHON_TYPE(cname, pyname, cstruct, doc)   \
  PyTypeObject cname = {                           \
      PyObject_HEAD_INIT(NULL)                     \
      0,                         /*ob_size*/       \
      pyname,                    /*tp_name*/       \
      sizeof(cstruct),           /*tp_basicsize*/  \
      0,                         /*tp_itemsize*/   \
      0,                         /*tp_dealloc*/    \
      0,                         /*tp_print*/      \
      0,                         /*tp_getattr*/    \
      0,                         /*tp_setattr*/    \
      0,                         /*tp_compare*/    \
      0,                         /*tp_repr*/       \
      0,                         /*tp_as_number*/  \
      0,                         /*tp_as_sequence*/\
      0,                         /*tp_as_mapping*/ \
      0,                         /*tp_hash */      \
      0,                         /*tp_call*/       \
      0,                         /*tp_str*/        \
      0,                         /*tp_getattro*/   \
      0,                         /*tp_setattro*/   \
      0,                         /*tp_as_buffer*/  \
      Py_TPFLAGS_DEFAULT,        /*tp_flags*/      \
      doc,                       /* tp_doc */      \
  }                                                

extern PyTypeObject BtrfsNodeType;
struct BtrfsNode {
  PyObject_HEAD;
  PyObject* uuid, *puuid, *ruuid;
  PyObject* creation_utc;

  struct root_info node;
};

int PyType_BtrfsNodeType_Ready();

PyObject* build_from_root_info(struct root_info* subvol);
PyObject* build_uuid_from_array(u8* uuid);
PyObject* build_datetime_from_ts(u64 ts);

void BtrfsNodeType__del__(struct BtrfsNode* self);
PyObject* BtrfsNodeType__repr__(struct BtrfsNode* self);

PyObject* is_snapshot(struct BtrfsNode* self);
PyObject* is_readonly(struct BtrfsNode* self);

PyObject* BtrfsNodeType__reduce__(struct BtrfsNode* self);
PyObject* BtrfsNodeType__setstate__(struct BtrfsNode* self, PyObject* arg_tuple);

#endif //__PYBTRFS_MOD_TYPE_H__

