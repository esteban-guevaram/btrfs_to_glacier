#include "pybtrfs_mod_type.h"

#include <python2.7/structmember.h>
#include <python2.7/datetime.h>

struct BtrfsNode {
  PyObject_HEAD;
  PyObject* uuid, *puuid;
  PyObject* creation_utc;

  struct root_info node;
};

PYTHON_TYPE(BtrfsNodeType, "pybtrfs.BtrfsNode", struct BtrfsNode, "Represents a subvolume information (name, uuid, path, ...)");

PyMethodDef BtrfsNodeMethods[] = {
  {"is_snapshot", (PyCFunction)is_snapshot, METH_NOARGS, "True is the subvolume is a snapshot"},
  {NULL}
};

PyMemberDef BtrfsNodeMembers[] = {
  {"name",         T_STRING, offsetof(struct BtrfsNode, node.name), READONLY, "subvolume name"},
  {"path",         T_STRING, offsetof(struct BtrfsNode, node.full_path), READONLY, "full path were subvolume is mounted"},
  {"uuid",         T_OBJECT, offsetof(struct BtrfsNode, uuid), READONLY, "the subvolume id"},
  {"puuid",        T_OBJECT, offsetof(struct BtrfsNode, puuid), READONLY, "the parent subvolume id"},
  {"creation_utc", T_OBJECT, offsetof(struct BtrfsNode, creation_utc), READONLY, "the datetime in utc when the subvolume was created"},
  {NULL}
};

int PyType_BtrfsNodeType_Ready() {
  PyDateTime_IMPORT;
  BtrfsNodeType.tp_members = BtrfsNodeMembers;
  BtrfsNodeType.tp_methods = BtrfsNodeMethods;
  BtrfsNodeType.tp_new = PyType_GenericNew;
  BtrfsNodeType.tp_dealloc = (destructor)BtrfsNodeType__del__;
  BtrfsNodeType.tp_repr = (reprfunc)BtrfsNodeType__repr__;
  return PyType_Ready(&BtrfsNodeType);
}

void BtrfsNodeType__del__(struct BtrfsNode* self) {
  TRACE("Destroying : %s", self->node.name ? self->node.name : "NONE" );
  clear_data_subvol(&self->node);
  Py_XDECREF(self->uuid);
  Py_XDECREF(self->puuid);
  Py_XDECREF(self->creation_utc);
  BtrfsNodeType.tp_free(self);
}

PyObject* BtrfsNodeType__repr__(struct BtrfsNode* self) {
  PyObject *path = NULL, *uuid = NULL, *creation_utc = NULL, *tuple = NULL;

  FAIL_AND_GOTO_IF(BtrfsNodeType__repr__clean,
    (path = PyObject_GetAttrString((PyObject*)self, "path")) == NULL);
  FAIL_AND_GOTO_IF(BtrfsNodeType__repr__clean,
    (uuid = PyObject_GetAttrString((PyObject*)self, "uuid")) == NULL);
  FAIL_AND_GOTO_IF(BtrfsNodeType__repr__clean,
    (creation_utc = PyObject_GetAttrString((PyObject*)self, "creation_utc")) == NULL);

  FAIL_AND_GOTO_IF(BtrfsNodeType__repr__clean,
    (tuple = PyTuple_Pack(3, path, uuid, creation_utc)) == NULL);
  PyObject* repr = PyString_Format(PyString_FromString("[%s, %r, %r]"), tuple);

  BtrfsNodeType__repr__clean:
  Py_XDECREF(tuple);
  return repr;
}

PyObject* is_snapshot (struct BtrfsNode* self) {
  if( self->node.root_offset > 0 )
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;  
}

PyObject* build_from_root_info(struct root_info* subvol) {
  PyObject* node = NULL;
  struct BtrfsNode* typedNode = NULL;

  FAIL_AND_GOTO_IF (build_from_root_info_clean,
    (node = PyObject_CallObject((PyObject*)&BtrfsNodeType, NULL)) == NULL );

  typedNode = (struct BtrfsNode*)node;

  FAIL_AND_GOTO_IF (build_from_root_info_clean,
    clone_subvol(subvol, &typedNode->node) != 0);
  FAIL_AND_GOTO_IF (build_from_root_info_clean,
    (typedNode->uuid = build_uuid_from_array(subvol->uuid)) == NULL );
  FAIL_AND_GOTO_IF (build_from_root_info_clean,
    (typedNode->puuid = build_uuid_from_array(subvol->puuid)) == NULL );
  FAIL_AND_GOTO_IF (build_from_root_info_clean,
    (typedNode->creation_utc = build_datetime_from_ts(subvol->otime)) == NULL );

  goto build_from_root_info_ok;

  build_from_root_info_clean:
  PY_SET_RUNTIME_ERR("Unexpected error at build_from_root_info");
  Py_XDECREF(node);
  node = NULL;

  build_from_root_info_ok:
  return node;
}

PyObject* build_uuid_from_array(u8* uuid) {
  if (!uuid) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  return PyString_FromStringAndSize((char*)uuid, BTRFS_UUID_SIZE);

  /*PyObject* tuple = NULL;
  FAIL_AND_GOTO_IF(build_uuid_from_array_clean,
    (tuple = PyTuple_New(BTRFS_UUID_SIZE)) ==  NULL);

  for(int i=0; i<BTRFS_UUID_SIZE; ++i) {
    PyObject* item = Py_BuildValue("i", uuid[i]);
    FAIL_AND_GOTO_IF(build_uuid_from_array_clean,
      !item || PyTuple_SetItem(tuple, i, item) != 0);
  }

  goto build_uuid_from_array_ok;
  build_uuid_from_array_clean:
  Py_XDECREF(tuple);
  tuple = NULL;
  build_uuid_from_array_ok:
  return tuple;*/
}

PyObject* build_datetime_from_ts(u64 ts) {
  if (!ts) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  struct tm *time_st = gmtime((time_t*)&ts);
  if (!time_st) {
    PyErr_SetString(PyExc_RuntimeError, "Failed when calling gmtime");
    return NULL;
  }

  TRACE("=> %d %d %d %d %d %d", 1900+time_st->tm_year, 1+time_st->tm_mon, time_st->tm_mday,
    time_st->tm_hour, time_st->tm_min, time_st->tm_sec);
  return PyDateTime_FromDateAndTime(1900+time_st->tm_year, 1+time_st->tm_mon, time_st->tm_mday,
    time_st->tm_hour, time_st->tm_min, time_st->tm_sec, 0);
}

