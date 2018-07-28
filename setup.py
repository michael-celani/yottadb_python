from distutils.core import setup, Extension

ydb_dist = '/usr/local/lib/yottadb/r122'
yotta = Extension('yotta', 
    sources = ['./module/yotta_module.c'], 
    include_dirs = [ydb_dist], 
    libraries = ['yottadb'], 
    library_dirs = [ydb_dist], 
    extra_compile_args = ["-O0"])

setup(name = 'YottaDB', 
version = '0.1', 
description = "Python bindings for YottaDB",
packages = ['pyyotta'],
ext_modules = [yotta])
