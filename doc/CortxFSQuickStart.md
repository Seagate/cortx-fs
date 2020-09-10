# CORTXFS QuickStart guide
CORTXFS provides file system infrastructure over clustered object store.
This file system infrastructure is suitable for file access protocols like
NFS, CIFS.
CORTXFS interacts with NSAL and DSAL libraries to provide file system like
infrastructure.

### Acrhitectural overview
![CORTXFS Architecture](../images/cortxfs-arch.jpg)

NSAL (Name Space Abstraction Layer) provides a way to create a global name
space, which can be accessed from any node in the clustered object store.
[Know more about NSAL](https://github.com/Seagate/cortx-nsal)

CORTXFS makes use of NSAL to create file system and export information (a.k.a.
endpoint). The directory structure is represented as kvtree in NSAL. The file
is represented via kvnode in NSAL, just like inode. 

DSAL (Data Store Abstraction Layer) provides a way to write, read and
modify the file data, from any node in the clustered object store. 
[Know more about DSAL](https://github.com/Seagate/cortx-dsal)

### Code structure
CORTXFS provides 2 major ways to deal with underneath file/directory objects.
There are functions which works with those objects directly.
There is another set of functions which works on those objects via file-handle.
This set of functions are more useful for NFS protocol.

