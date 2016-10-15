#include "pybtrfs_mod_type.h"
#include "pybtrfs_mod_function.h"

#include <structmember.h>
#include <datetime.h>

PYTHON_TYPE(BtrfsNodeType, "pybtrfs.BtrfsNode", struct BtrfsNode, "Represents a subvolume information (name, uuid, path, ...)");

PyMethodDef BtrfsNodeMethods[] = {
  {"is_snapshot", (PyCFunction)is_snapshot, METH_NOARGS, "True is the subvolume is a snapshot"},
  {"is_readonly", (PyCFunction)is_readonly, METH_NOARGS, "True is the subvolume is readonly"},
  {"__reduce__", (PyCFunction)BtrfsNodeType__reduce__, METH_NOARGS, "Used to pickle instances of BtrfsNode"},
  {"__setstate__", (PyCFunction)BtrfsNodeType__setstate__, METH_VARARGS, "Restores the subvol data from pickle"},
  {NULL}
};

PyMemberDef BtrfsNodeMembers[] = {
  // name and path must be utf-8 encoded (i think)
  {"name",         T_STRING, offsetof(struct BtrfsNode, node.name), READONLY, "subvolume name"},
  {"path",         T_STRING, offsetof(struct BtrfsNode, node.full_path), READONLY, "full path were subvolume is mounted"},
  {"uuid",         T_OBJECT, offsetof(struct BtrfsNode, uuid), READONLY, "the subvolume id"},
  {"puuid",        T_OBJECT, offsetof(struct BtrfsNode, puuid), READONLY, "the parent subvolume id"},
  {"ruuid",        T_OBJECT, offsetof(struct BtrfsNode, ruuid), READONLY, "the received subvolume id"},
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
  PyObject *name = NULL, *uuid = NULL, *creation_utc = NULL, 
           *utc_str = NULL, *uuid_str = NULL, *tuple = NULL, *repr = NULL;
  char buffer[BTRFS_UUID_SIZE*2 + 1] = {'\0'};         

  GOTO_IF_NULL(BtrfsNodeType__repr__clean,
    name = PyObject_GetAttrString((PyObject*)self, "name") );
  GOTO_IF_NULL(BtrfsNodeType__repr__clean,
    uuid = PyObject_GetAttrString((PyObject*)self, "uuid") );
  GOTO_IF_NULL(BtrfsNodeType__repr__clean,
    creation_utc = PyObject_GetAttrString((PyObject*)self, "creation_utc") );

  uuid_to_str((u8*)PyBytes_AsString(uuid), buffer);
  GOTO_IF_NULL(BtrfsNodeType__repr__clean,
    utc_str = PyObject_CallMethod(creation_utc, "strftime", "s", "%Y/%m/%d %H:%M:%S") );
  GOTO_IF_NULL(BtrfsNodeType__repr__clean,
    uuid_str = PyUnicode_FromString(buffer) );

  GOTO_IF_NULL(BtrfsNodeType__repr__clean,
    tuple = PyTuple_Pack(3, name, uuid_str, utc_str) );
  repr = PyUnicode_Format(PyUnicode_FromString("[%s, %r, %r]"), tuple);

  BtrfsNodeType__repr__clean:
  Py_XDECREF(tuple);
  Py_XDECREF(utc_str);
  Py_XDECREF(uuid_str);
  return repr;
}

PyObject* is_snapshot (struct BtrfsNode* self) {
  if( self->node.root_offset > 0 )
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;  
}

PyObject* is_readonly (struct BtrfsNode* self) {
  if( self->node.flags & BTRFS_ROOT_SUBVOL_RDONLY )
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;  
}

PyObject* build_from_root_info(struct root_info* subvol) {
  PyObject* node = NULL;
  struct BtrfsNode* typedNode = NULL;

  GOTO_IF_NULL (build_from_root_info_clean,
    node = PyObject_CallObject((PyObject*)&BtrfsNodeType, NULL) );

  typedNode = (struct BtrfsNode*)node;

  FAIL_AND_GOTO_IF (build_from_root_info_clean,
    clone_subvol(subvol, &typedNode->node) != 0);
  GOTO_IF_NULL (build_from_root_info_clean,
    typedNode->uuid = build_uuid_from_array(subvol->uuid) );
  GOTO_IF_NULL (build_from_root_info_clean,
    typedNode->puuid = build_uuid_from_array(subvol->puuid) );
  GOTO_IF_NULL (build_from_root_info_clean,
    typedNode->ruuid = build_uuid_from_array(subvol->ruuid) );
  GOTO_IF_NULL (build_from_root_info_clean,
    typedNode->creation_utc = build_datetime_from_ts(subvol->otime) );

  //assert(typedNode->puuid && typedNode->puuid != Py_None);
  goto build_from_root_info_ok;

  build_from_root_info_clean:
  PY_SET_RUNTIME_ERR("Unexpected error at build_from_root_info");
  Py_XDECREF(node);
  node = NULL;

  build_from_root_info_ok:
  return node;
}

static u32 uuid_not_null(u8* uuid) {
  u32 result = 0;
  for (u32 i=0; i<BTRFS_UUID_SIZE; ++i)
    result += uuid[i];
  return result;  
}

PyObject* build_uuid_from_array(u8* uuid) {
  if (!uuid || !uuid_not_null(uuid)) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  return PyBytes_FromStringAndSize((char*)uuid, BTRFS_UUID_SIZE);

  /*PyObject* tuple = NULL;
  GOTO_IF_NULL(build_uuid_from_array_clean,
    tuple = PyTuple_New(BTRFS_UUID_SIZE) );

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

PyObject* BtrfsNodeType__reduce__(struct BtrfsNode* self) {
  PyObject *result = NULL, *packed = NULL;

  GOTO_IF_NULL(BtrfsNodeType__reduce__fail,
    packed = pack_subvol_c_struct((PyObject*)self) );
  GOTO_IF_NULL(BtrfsNodeType__reduce__fail,
    result = PyTuple_New(3) );

  Py_INCREF((PyObject*)&BtrfsNodeType);
  FAIL_AND_GOTO_IF(BtrfsNodeType__reduce__fail,
    PyTuple_SetItem(result, 0, (PyObject*)&BtrfsNodeType) != 0);

  FAIL_AND_GOTO_IF(BtrfsNodeType__reduce__fail,
    PyTuple_SetItem(result, 1, PyTuple_New(0)) != 0);
  FAIL_AND_GOTO_IF(BtrfsNodeType__reduce__fail,
    PyTuple_SetItem(result, 2, packed) != 0);

  goto BtrfsNodeType__reduce__ok;
  BtrfsNodeType__reduce__fail:
  Py_XDECREF(result);
  result = NULL;
  BtrfsNodeType__reduce__ok:
  return result;
}

PyObject* BtrfsNodeType__setstate__(struct BtrfsNode* self, PyObject* arg_tuple) {
  PyObject *packed = NULL;

  FAIL_AND_GOTO_IF(BtrfsNodeType__setstate__fail,
    PyArg_ParseTuple(arg_tuple, "S", &packed) == 0);
  GOTO_IF_NULL(BtrfsNodeType__setstate__fail,
    unpack_subvol_c_struct((PyObject*)self, packed) );

  GOTO_IF_NULL (BtrfsNodeType__setstate__fail,
    self->uuid = build_uuid_from_array(self->node.uuid) );
  GOTO_IF_NULL (BtrfsNodeType__setstate__fail,
    self->puuid = build_uuid_from_array(self->node.puuid) );
  GOTO_IF_NULL (BtrfsNodeType__setstate__fail,
    self->ruuid = build_uuid_from_array(self->node.ruuid) );
  GOTO_IF_NULL (BtrfsNodeType__setstate__fail,
    self->creation_utc = build_datetime_from_ts(self->node.otime) );

  Py_INCREF(Py_None);
  return Py_None;

  BtrfsNodeType__setstate__fail:
  return NULL;
}

