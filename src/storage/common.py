#!/usr/bin/env python3

from src.storage import fuse, http, s3, xrd
from src.storage.utils import storage_init

params = storage_init()
fs_backend, krb5_enabled = params['fs_backend'], params['krb5_enabled']

fs_backends = ['eos', 'lustre', 'nfs', 'xrootd', 'http', 's3', 'fuse']

fs_mod = None

if fs_backend == 'xrootd' or fs_backend == "eos":
    fs_mod = xrd
elif fs_backend == 'lustre' or fs_backend == 'nfs' or fs_backend == 'fuse' :
    fs_mod = fuse
elif fs_backend == 'http':
    fs_mod = http
elif fs_backend == 's3':
    fs_mod = s3
else:
    raise ValueError(f'fs_backend {fs_backend} is unknown!')

#### Export method
mkdir = fs_mod.mkdir
list_dir = fs_mod.list_dir
upload_file = fs_mod.upload_file
cat_file = fs_mod.cat_file
get_file = fs_mod.get_file
get_file_stream = fs_mod.get_file_stream
delete_path = fs_mod.delete_path
path_exist = fs_mod.path_exist
chmod = fs_mod.chmod
rename = fs_mod.rename
init_ink_space = fs_mod.init_ink_space

if __name__ == "__main__":
    pass
