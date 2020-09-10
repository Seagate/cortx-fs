# CORTXFS QuickStart guide
CORTXFS provides file system infrastructure over clustered object store.
This file system infrastructure is suitable for file access protocols like
NFS, CIFS.
CORTXFS interacts with NSAL and DSAL libraries to provide file system like
infrastructure. NSAL (Name Space Abstraction Layer) provides a way to create a
global name space, which can be accessed from any node in the clustered object
store. DSAL (Data Store Abstraction Layer) provides a way to write, read and
modify the file data, from any node in the clustered object store. 
[Know more about NSAL](https://github.com/Seagate/cortx-nsal)
[Know more about DSAL](https://github.com/Seagate/cortx-dsal)

### Acrhitectural overview
![CORTXFS Architecture](../images/cortxfs-arch.jpg)

### Code structure
