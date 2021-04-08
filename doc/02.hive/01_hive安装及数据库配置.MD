# Hive安装

1. 上传文件

2. 解压文件并修改文件名

   ```shell	
   tar -zxvf /opt/software/apache-hive-3.1.2-bin.tar.gz -C /opt/module/
   
   mv /opt/module/apache-hive-3.1.2-bin/ /opt/module/hive
   ```

3. 添加环境变量

   ```shell
   sudo vim /etc/profile.d/my_env.sh
    
   #HIVE_HOME
   export HIVE_HOME=/opt/module/hive
   export PATH=$PATH:$HIVE_HOME/bin
   ```

4. 刷新配置文件

5. 解决日志Jar包冲突

   ```shell
   mv $HIVE_HOME/lib/log4j-slf4j-impl-2.10.0.jar $HIVE_HOME/lib/log4j-slf4j-impl-2.10.0.bak
   ```

6. 初始化元数据库

   ```shell
   bin/schematool -dbType derby -initSchema
   ```

7. 启动Hive

   ```shell
   bin/hive
   ```

8. 使用Hive

   ```shell
   hive> show databases;
   hive> show tables;
   hive> create table test(id int);
   hive> insert into test values(1);
   hive> select * from test;
   ```

**注意：**

Hive 默认使用的元数据库为 derby，开启 Hive 之后就会占用元数据库，且不与其他客户端共享数据，所以我们需要将 Hive 的元数据地址改为 MySQL。

# Hive元数据配置到MySQL

1. 拷贝驱动

   ```shell
   cp /opt/software/mysql-connector-java-5.1.37.jar $HIVE_HOME/lib
   ```

2. 配置Metastore到MySQL

   1. 在$HIVE_HOME/conf 目录下新建hive-site.xml文件

      ```shell
      vim $HIVE_HOME/conf/hive-site.xml
      ```

   2. 添加如下内容

      ```xml
      <?xml version="1.0"?>
      <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      <configuration>
       <!-- jdbc 连接的 URL -->
       <property>
       <name>javax.jdo.option.ConnectionURL</name>
       <value>jdbc:mysql://192.168.111.91:33336/metastore?useSSL=false</value>
      </property>
       <!-- jdbc 连接的 Driver-->
       <property>
       <name>javax.jdo.option.ConnectionDriverName</name>
       <value>com.mysql.jdbc.Driver</value>
      </property>
      <!-- jdbc 连接的 username-->
       <property>
       <name>javax.jdo.option.ConnectionUserName</name>
       <value>root</value>
       </property>
       <!-- jdbc 连接的 password -->
       <property>
       <name>javax.jdo.option.ConnectionPassword</name>
       <value>GGSbKL3N1ninKCrZ</value>
      </property>
       <!-- Hive 元数据存储版本的验证 -->
       <property>
       <name>hive.metastore.schema.verification</name>
       <value>false</value>
      </property>
       <!--元数据存储授权-->
       <property>
       <name>hive.metastore.event.db.notification.api.auth</name>
       <value>false</value>
       </property>
       <!-- Hive 默认在 HDFS 的工作目录 -->
       <property>
           <name>hive.metastore.warehouse.dir</name>
       <value>/user/hive/warehouse</value>
       </property>
      </configuration>
      ```

3. 在数据库中创建metastore数据库

   ```shell
   create database metastore;
   ```

4. 初始化Hive元数据库

   ```shell
   bin/schematool -initSchema -dbType mysql -verbose
   ```

5. 启动Hive

   ```shell
   bin/hive
   ```

# 使用元数据服务的方式访问Hive

1. 在hive-site.xml文件中添加如下配置信息

   ```xml
   <!-- 指定存储元数据要连接的地址 -->
    <property>
    	<name>hive.metastore.uris</name>
    	<value>thrift://hadoop01:9083</value>
    </property>
   ```

2. 启动metastore

   ```shell
   hive --service metastore
   ```

3. 启动Hive

   ```shell
   bin/hive
   ```

# 使用JDBC的方式访问Hive

1. 在hive-site.xml文件中添加如下配置信息

   ```shell
   <!-- 指定 hiveserver2 连接的 host -->
    <property>
    <name>hive.server2.thrift.bind.host</name>
     <value>hadoop01</value>
    </property>
    <!-- 指定 hiveserver2 连接的端口号 -->
    <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
    </property>
   ```

2. 启动hiveserver2

   ```shell
   bin/hive --service hiveserver2
   ```

3. 启动beeline客户端（需要多等待一会）

   ```shell
   bin/beeline -u jdbc:hive2://master:10000 -n damoncai
   ```

4. 看到如下界面

   ```shell
   Connecting to jdbc:hive2://hadoop102:10000
   Connected to: Apache Hive (version 3.1.2)
   Driver: Hive JDBC (version 3.1.2)
   Transaction isolation: TRANSACTION_REPEATABLE_READ
   Beeline version 3.1.2 by Apache Hive
   0: jdbc:hive2://hadoop01:10000>
   ```

**注意：**

​			在使用hiveserver2时需先启动metastore

