#include "Python.h"
#include "libyottadb.h"
#include "libydberrors.h"
#include "libydberrors2.h"
#include "string.h"

// == Exceptions ==

typedef struct ydb_exception_definition {
    PyObject **exception;
    PyObject *base;
    const char* name;
    const char* obj_name;
    const char* docstring;
} ydb_exception_definition;

static PyObject *YottaError;
static PyObject *UndefinedGlobalError;

static void cleanup_failed_exceptions()
{
    Py_XDECREF(YottaError);
    Py_XDECREF(UndefinedGlobalError);
}

static int initialize_exception(PyObject *module, ydb_exception_definition def)
{
    *(def.exception) = PyErr_NewExceptionWithDoc(def.name, def.docstring, def.base, NULL);

    if (!*(def.exception)) {
        cleanup_failed_exceptions();
        return 0;
    }

    Py_INCREF(*(def.exception));
    PyModule_AddObject(module, def.obj_name, *(def.exception));
    return 1;
}

static int initialize_yotta_exceptions(PyObject *module)
{
    ydb_exception_definition error_def;

    error_def.exception = &YottaError;
    error_def.base = NULL;
    error_def.obj_name = "YottaError";
    error_def.name = "yotta.YottaError";
    error_def.docstring = "Base exception class for the YDB binding.";
    if (!initialize_exception(module, error_def)) {
        return 0;
    }

    error_def.exception = &UndefinedGlobalError;
    error_def.base = YottaError;
    error_def.obj_name = "UndefinedGlobalError";
    error_def.name = "yotta.UndefinedGlobalError";
    error_def.docstring = "This indicates that the program attempted to evaluate an undefined global variable.";
    if (!initialize_exception(module, error_def)) {
        return 0;
    }

    return 1;
}

static void raise_yotta_exception(int yotta_ret)
{
    switch (yotta_ret)
    {
        case YDB_ERR_GVUNDEF:
            PyErr_SetString(UndefinedGlobalError, "attempted to evaluate an undefined global variable");
        break;
        
        default:
            PyErr_Format(YottaError, 
                    "unimplemented exception from YottaDB: code %i", 
                    yotta_ret);
        break;
    }
}

// == End Exceptions ==

// == Type Conversions ==

typedef struct ydb_buffer_list {
    int num_buffers;
    ydb_buffer_t *ydb_buffers;
} ydb_buffer_list;

static int as_ydb_buffer(PyObject *obj, ydb_buffer_t *buf)
{
    if (!PyObject_TypeCheck(obj, &PyUnicode_Type))
    {
        PyErr_Format(PyExc_TypeError, 
                    "'%s' object is not a string", 
                    Py_TYPE(obj)->tp_name);
        return 0;
    }

    Py_ssize_t str_len;
    const char *str = PyUnicode_AsUTF8AndSize(obj, &str_len);

    if (!str)
    {
        return 0;
    }

    buf->buf_addr = str;
    buf->len_used = str_len;
    buf->len_alloc = str_len;

    return 1;
}

static int as_ydb_buffer_list(PyObject *obj, ydb_buffer_list *buf_list)
{
    // Cleanup
    if (!obj)
    {
        free(buf_list->ydb_buffers);
        return 0;
    }
    
    if (!PySequence_Check(obj))
    {
        PyErr_Format(PyExc_TypeError, 
                    "'%s' object is not a sequence", 
                    Py_TYPE(obj)->tp_name);
        return 0;
    }

    buf_list->num_buffers = (int) PySequence_Size(obj);
    buf_list->ydb_buffers = (ydb_buffer_t *) malloc(sizeof(ydb_buffer_t) * buf_list->num_buffers);

    for (int list_idx = 0; list_idx < buf_list->num_buffers; list_idx++)
	{
		PyObject *str_obj = PySequence_GetItem(obj, list_idx);
        
        if (!as_ydb_buffer(str_obj, buf_list->ydb_buffers + list_idx))
        {
            Py_DECREF(str_obj);
            free(buf_list->ydb_buffers);
            return 0;
        }

        Py_DECREF(str_obj);
    }

    return Py_CLEANUP_SUPPORTED;
}

static PyObject *buffer_list_as_tuple(ydb_buffer_list *buf_list)
{
    PyObject *ret_tuple = PyTuple_New(buf_list->num_buffers);
    if (!ret_tuple)
    {
        return NULL;
    }

    for (int i = 0; i < buf_list->num_buffers; i++)
    {
        ydb_buffer_t *ret_buf = buf_list->ydb_buffers + i;

        PyObject *ret_string = PyUnicode_DecodeUTF8(ret_buf->buf_addr, ret_buf->len_used, NULL);
        if (!ret_string)
        {
            for (int j = 0; j < i - 1; j++)
            {
                Py_DECREF(PyTuple_GetItem(ret_tuple, j));
            }
            Py_DECREF(ret_tuple);
            return NULL;
        }

        if (PyTuple_SetItem(ret_tuple, i, ret_string))
        {
            Py_DECREF(ret_string);
            for (int j = 0; j < i - 1; j++)
            {
                Py_DECREF(PyTuple_GetItem(ret_tuple, j));
            }
            Py_DECREF(ret_tuple);
            return NULL; 
        }
    }

    return ret_tuple;
}

// == End Type Conversions ==

// == YottaDB Bindings ==

static PyObject* yotta_ydb_data_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_list sublist;
    unsigned int data_ret;

	if (!PyArg_ParseTuple(args, "O&O&", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist))
	{
		return NULL;
	}

    int ret = ydb_data_s(&varname, sublist.num_buffers, sublist.ydb_buffers, &data_ret);
    free(sublist.ydb_buffers);
    
    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    return PyLong_FromUnsignedLong((unsigned long) data_ret);
}

static PyObject* yotta_ydb_delete_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_list sublist;
    unsigned int deltype;

	if (!PyArg_ParseTuple(args, "O&O&i", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist, &deltype))
	{
		return NULL;
	}

    int ret = ydb_delete_s(&varname, sublist.num_buffers, sublist.ydb_buffers, deltype);
    free(sublist.ydb_buffers);
    
    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject* yotta_ydb_delete_excl_s(PyObject *self, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject* yotta_ydb_get_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_t valname;
    ydb_buffer_list sublist;

	if (!PyArg_ParseTuple(args, "O&O&", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist)) 
	{
		return NULL;
	}

    // Value:
    char value_str[YDB_MAX_STR];
    valname.buf_addr = &value_str;
    valname.len_alloc = YDB_MAX_STR;

    int ret = ydb_get_s(&varname, sublist.num_buffers, sublist.ydb_buffers, &valname);
    free(sublist.ydb_buffers);
    
    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    return PyUnicode_DecodeUTF8(valname.buf_addr, valname.len_used, NULL);
}

static PyObject* yotta_ydb_incr_s(PyObject *selfx, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject* yotta_ydb_lock_s(PyObject *selfx, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject *yotta_ydb_lock_decr_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_list sublist;

	if (!PyArg_ParseTuple(args, "O&O&", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist))
	{
		return NULL;
	}
    
    int ret = ydb_lock_decr_s(&varname, sublist.num_buffers, sublist.ydb_buffers);
    free(sublist.ydb_buffers);

    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    Py_RETURN_NONE;   
}

static PyObject *yotta_ydb_lock_incr_s(PyObject *self, PyObject *args)
{
    unsigned long long timeout_sec;
    ydb_buffer_t varname;
    ydb_buffer_list sublist;

	if (!PyArg_ParseTuple(args, "KO&O&", &timeout_sec, &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist))
	{
		return NULL;
	}
    
    int ret = ydb_lock_incr_s(timeout_sec, &varname, sublist.num_buffers, sublist.ydb_buffers);
    free(sublist.ydb_buffers);

    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    Py_RETURN_NONE;   
}

static PyObject *yotta_ydb_node_next_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_list sublist;

	if (!PyArg_ParseTuple(args, "O&O&", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist))
	{
		return NULL;
	}

    ydb_buffer_list retlist;
    ydb_buffer_t buffers[YDB_MAX_SUBS];
    retlist.num_buffers = YDB_MAX_SUBS;
    retlist.ydb_buffers = &buffers;

    for (int i = 0; i < YDB_MAX_SUBS; i++)
    {
        ydb_buffer_t *buff = retlist.ydb_buffers + i;

        buff->buf_addr = (char *) malloc(sizeof(char) * YDB_MAX_STR + 1);
        buff->len_alloc = YDB_MAX_STR;
    }

    int ret = ydb_node_next_s(&varname, sublist.num_buffers, sublist.ydb_buffers, &retlist.num_buffers, retlist.ydb_buffers);
    free(sublist.ydb_buffers);

    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        for (int i = 0; i < YDB_MAX_SUBS; i++)
        {
            ydb_buffer_t *buff = retlist.ydb_buffers + i;
            free(buff->buf_addr);
        }
        return NULL;
    }

    PyObject *ret_obj = NULL;
    if (retlist.num_buffers == YDB_NODE_END)
    {
        Py_INCREF(Py_None);
        ret_obj = Py_None;
    } 
    else
    {
        ret_obj = buffer_list_as_tuple(&retlist);
    }
    
    for (int i = 0; i < YDB_MAX_SUBS; i++)
    {
        ydb_buffer_t *buff = retlist.ydb_buffers + i;
        free(buff->buf_addr);
    }
    
    return ret_obj;
}

static PyObject *yotta_ydb_node_previous_s(PyObject *self, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject *yotta_ydb_set_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_t valname;
    ydb_buffer_list sublist;

	if (!PyArg_ParseTuple(args, "O&O&O&", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist, &as_ydb_buffer, &valname))
	{
		return NULL;
	}
    
    int ret = ydb_set_s(&varname, sublist.num_buffers, sublist.ydb_buffers, &valname);
    free(sublist.ydb_buffers);

    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *yotta_str2zwr_s(PyObject *self, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject *yotta_ydb_subscript_next_s(PyObject *self, PyObject *args)
{
    ydb_buffer_t varname;
    ydb_buffer_t valname;
    ydb_buffer_list sublist;

	if (!PyArg_ParseTuple(args, "O&O&", &as_ydb_buffer, &varname, &as_ydb_buffer_list, &sublist)) 
	{
		return NULL;
	}

    char value_str[YDB_MAX_STR];
    valname.buf_addr = &value_str;
    valname.len_alloc = YDB_MAX_STR;

    int ret = ydb_subscript_next_s(&varname, sublist.num_buffers, sublist.ydb_buffers, &valname);
    free(sublist.ydb_buffers);

    if (ret != YDB_OK)
    {
        raise_yotta_exception(ret);
        return NULL;
    }

    return PyUnicode_DecodeUTF8(valname.buf_addr, valname.len_used, NULL);
}

static PyObject *yotta_ydb_subscript_previous_s(PyObject *self, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject *yotta_ydb_tp_s(PyObject *self, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject *yotta_ydb_zwr2str_s(PyObject *self, PyObject *args)
{
    Py_RETURN_NOTIMPLEMENTED;
}

// == End YottaDB Bindings ==

// == Module Definitions ==

static PyMethodDef methods[] = {
    {"ydb_data_s", yotta_ydb_data_s, METH_VARARGS, "Get information about YottaDB data."},
    {"ydb_delete_s", yotta_ydb_delete_s, METH_VARARGS, "Delete a value from YottaDB."},
	{"ydb_set_s", yotta_ydb_set_s, METH_VARARGS, "Set a value into YottaDB."},
    {"ydb_lock_decr_s", yotta_ydb_lock_decr_s, METH_VARARGS, "Locks a YottaDB node."},
    {"ydb_lock_incr_s", yotta_ydb_lock_incr_s, METH_VARARGS, "Unlocks a YottaDB node."},
	{"ydb_get_s", yotta_ydb_get_s, METH_VARARGS, "Get a value from YottaDB."},
    {"ydb_subscript_next_s", yotta_ydb_subscript_next_s, METH_VARARGS, "Orders on a YottaDB node's subscripts."},
    {"ydb_node_next_s", yotta_ydb_node_next_s, METH_VARARGS, "Queries on a YottaDB node's subscripts."},
	{NULL, NULL, 0, NULL}
};

static struct PyModuleDef yottamodule = {
	PyModuleDef_HEAD_INIT,
	"yotta",
	NULL,
	-1,
	methods
};

PyMODINIT_FUNC PyInit_yotta(void)
{
    PyObject *module = PyModule_Create(&yottamodule);
    if (!module)
    {
        return NULL;
    }

    if (!initialize_yotta_exceptions(module))
    {
        Py_DECREF(module);
        return NULL;
    }

    return module;
}

// == End Module Definitions ==