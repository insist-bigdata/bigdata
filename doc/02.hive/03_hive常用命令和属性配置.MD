# Hive常用交互命令

**bin/hive -help**

```shell
usage: hive
-d,--define <key=value> Variable subsitution to apply to hive
 commands. e.g. -d A=B or --define A=B
 --database <databasename> Specify the database to use
-e <quoted-query-string> SQL from command line
-f <filename> SQL from files
-H,--help Print help information
 --hiveconf <property=value> Use value for given property
 --hivevar <key=value> Variable subsitution to apply to hive
 commands. e.g. --hivevar A=B
-i <filename> Initialization SQL file
-S,--silent Silent mode in interactive shell
-v,--verbose Verbose mode (echo executed SQL to the console)
```

1. "-e"不进入hive的交互窗口执行sql语句

   ```shell
   bin/hive -e "select id from student;"
   ```

2. "-f"执行脚本中sql语句



# Hive其他操作命令

1. 退出窗口

   ```
   hive(default)>exit;
   hive(default)>quit;
   ```

2. 在hive cli命令窗口中如何查看hdfs文件系统

   ```shell
   hive(default)>dfs -ls /;
   ```



# Hive常见属性配置

### 日志信息配置

1. 修改hive/conf/hive-log4j2.properties.template文件名为hive-log4j2.properties

   ```shell
   mv hive-log4j2.properties.template hive-log4j2.properties
   ```

2. 在 hive-log4j2.properties 文件中修改 log 存放位置

   ```shell
   hive.log.dir=/opt/module/hive/logs
   ```

### 打印当前库和表头

在 hive-site.xml 中加入如下两个配置：

```xml
 <property>
 	<name>hive.cli.print.header</name>
 	<value>true</value>
 </property>
 <property>
 	<name>hive.cli.print.current.db</name>
 	<value>true</value>
 </property>
```



### 参数配置

1. 查看当前所有的配置信息

   ```shell
   hive>set;
   ```

2. 参数的配置三种方式

   1. 配置文件方式：

      默认配置文件hive-default.xml  

      用户自定义配置文件hive-site.xml

   2. 命令行参数方式

      启动 Hive 时，可以在命令行添加-hiveconf param=value 来设定参数。

      ```shell
      # 仅对本次hive启动有效
      bin/hive -hiveconf mapred.reduce.tasks=10;
      ```

      查看参数设置

      ```shell
      hive (default)> set mapred.reduce.tasks;
      ```

   3. 参数声明方式

      可以在HQL中使用SET关键字设定参数

      ```shell
      # 仅对本次hive启动有效
      hive (default)> set mapred.reduce.tasks=100;
      ```

      查看参数设置

      ```shell
      hive (default)> set mapred.reduce.tasks;
      ```

      