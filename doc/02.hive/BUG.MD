# beeline连接hiveserver2报错：User: root is not allowed to impersonate root
https://blog.csdn.net/qq_16633405/article/details/82190440
```xml
<property>
    <name>hadoop.proxyuser.root.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.root.groups</name>
    <value>*</value>
</property>
```

```xml

<property>
    <name>hadoop.proxyuser.damoncai.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.damoncai.groups</name>
    <value>*</value>
</property>
```