
# gplargecopy
Utility for copy tables between Greenplum clusters

# Usage
The configuration can be set either through arguments or environment variables. 
* **-all** - Copy all tables from all schemas
* **-batch** - Batch size, amount of rows copied at one time
* **-db-*** - Db connection params

**.env** file:

```shell
DB_URL_SRC=127.0.0.1
DB_USER_SRC=gpadmin
DB_PASSWORD_SRC=password
DB_NAME_SRC=dontepanlin
DB_TABLE_SRC=dontepanlin
DB_URL_DST=172.17.0.1
DB_USER_DST=gpadmin
DB_PASSWORD_DST=password
DB_NAME_DST=dontepanlin
DB_TABLE_DST=dontepanlin
```