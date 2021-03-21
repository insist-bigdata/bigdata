# 数据库操作

### 创建数据库

```shell
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];
```

1. 默认路径：/user/hive/warehouse/\*.db
2. 指定HDFS上存放位置：create database db_hive2 location '/db_hive2.db';

### 查看数据库详情

1. 显示数据库信息

   ```shell
   desc database db_hive;
   ```

2. 显示数据库详细信息，extended

   ```shell
   desc database extended db_hive;
   ```

### 切换数据库

```shell
use db_hive;
```

### 修改数据库

```shell
hive (default)> alter database db_hive set dbproperties('createtime'='20170830');
```

### 删除数据库

```shell
hive>drop database db_hive2;
```

**如果数据库不为空，可以采用cascade命令**

```shell
hive> drop database db_hive cascade;
```

# 表操作

### 建表语法

```shell
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name
[(col_name data_type [COMMENT col_comment], ...)]
[COMMENT table_comment]
[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
[CLUSTERED BY (col_name, col_name, ...)
[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
[ROW FORMAT row_format]
[STORED AS file_format]
[LOCATION hdfs_path]
[TBLPROPERTIES (property_name=property_value, ...)]
[AS select_statement]
```

1. EXTERNAL：关键字可以让用户创建一个外部表，在建表的同时可以指定一个指向实际数据的路径（LOCATION），在删除表的时候，内部表的元数据和数据会被一起删除，而外部表只删除元数据，不删除数据。 

2. COMMENT：为表和列添加注释。

3. PARTITIONED BY：创建分区表。

4. CLUSTERED BY： 创建分桶表。

5. SORTED BY：不常用，对桶中的一个或多个列另外排序。

6. ROW FORMAT：Hive 通过 SerDe 确定表的具体的列的数据。SerDe 是 Serialize/Deserilize 的简称， hive 使用 Serde 进行行对象的序列与反序列化。

7. STORED AS：指定存储文件类型

   常用的存储文件类型：SEQUENCEFILE（二进制序列文件）、TEXTFILE（文本）、RCFILE（列式存储格式文件）

   如果文件数据是纯文本，可以使用STORED AS TEXTFILE。如果数据需要压缩，使用 STORED AS SEQUENCEFILE。 

8. LOCATION ：指定表在 HDFS 上的存储位置。

9. AS：后跟查询语句，根据查询结果创建表。 

10. LIKE 允许用户复制现有的表结构，但是不复制数据。

### 管理表

默认创建的表都是所谓的管理表，有时也被称为内部表。因为这种表，Hive 会（或多或少地）控制着数据的生命周期。Hive 默认情况下会将这些表的数据存储在由配置项hive.metastore.warehouse.dir(例如，/user/hive/warehouse)所定义的目录的子目录下。

**当我们删除一个管理表时，Hive 也会删除这个表中数据。管理表不适合和其他工具共享数据。**

### 外部表

因为表是外部表，所以 Hive 并非认为其完全拥有这份数据。删除该表并不会删除掉这份数据，不过描述表的元数据信息会被删除掉。

**语法：**

```sql
create external table if not exists dept(
deptno int,
dname string,
loc int
)
row format delimited fields terminated by '\t';
```

### 管理表和外部表相互转换

1. 修改内部表 student2 为外部表

   ```shell
   alter table student2 set tblproperties('EXTERNAL'='TRUE');
   ```

2. 修改外部表 student2 为内部表

   ```shell
   alter table student2 set tblproperties('EXTERNAL'='FALSE');
   ```

3. 查看表类型

   ```shell
   hive (default)> desc formatted student2;
   Table Type: MANAGED_TABLE
   ```

   **注意：('EXTERNAL'='TRUE')和('EXTERNAL'='FALSE')为固定写法，区分大小写！**

### 修改表

**重命名表**

```sql
ALTER TABLE table_name RENAME TO new_table_name
```

**增加、修改和删除表分区**

1. 更新列

   ```sql
   ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]
   ```

2. 新增和替换列

   ```sql
   ALTER TABLE table_name ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...)
   ```

   **注：ADD 是代表新增一字段，字段位置在所有列后面(partition 列前)**

   **REPLACE 则是表示替换表中所有字段**

   

