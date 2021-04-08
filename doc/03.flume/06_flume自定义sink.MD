# 自定义sink

MySink 需要继承 AbstractSink 类并实现 Configurable 接口

实现相应方法：

configure(Context context)//初始化 context（读取配置文件内容）

process()//从 Channel 读取获取数据（event），这个方法将被循环调用。

使用场景：读取 Channel 数据写入 MySQL 或者其他文件系统。

### 需求

使用 flume 接收数据，并在 Sink 端给每条数据添加前缀和后缀，输出到控制台。前后缀可在 flume 任务配置文件中配置

**流程分析：**

![](./images/22.jpg)

**编码**

```shell
package com.atguigu;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {
 //创建 Logger 对象
 private static final Logger LOG = 
LoggerFactory.getLogger(AbstractSink.class);
 private String prefix;
 private String suffix;
 @Override
 public Status process() throws EventDeliveryException {
 //声明返回值状态信息
 Status status;
 //获取当前 Sink 绑定的 Channel
 Channel ch = getChannel();
 //获取事务
 Transaction txn = ch.getTransaction();
 //声明事件
 Event event;
 //开启事务
 txn.begin();
 //读取 Channel 中的事件，直到读取到事件结束循环
 while (true) {
 event = ch.take();
 if (event != null) {
 break;
 }
 }
 try {
 //处理事件（打印）
 LOG.info(prefix + new String(event.getBody()) + suffix);
 //事务提交
 txn.commit();
 status = Status.READY;
 } catch (Exception e) {
 //遇到异常，事务回滚
 txn.rollback();
 status = Status.BACKOFF;
 } finally {
 //关闭事务
 txn.close();
 }
 return status;
 }
 @Override
 public void configure(Context context) {
  //读取配置文件内容，有默认值
 prefix = context.getString("prefix", "hello:");
 //读取配置文件内容，无默认值
 suffix = context.getString("suffix");
 } 
 }
```

**打包**

将写好的代码打包，并放到 flume 的 lib 目录（/opt/module/flume）下

**配置文件**

```shell
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1
# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444
# Describe the sink
a1.sinks.k1.type = com.atguigu.MySink
#a1.sinks.k1.prefix = atguigu:
a1.sinks.k1.suffix = :atguigu
# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100
# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

**开启任务**

```shell
bin/flume-ng agent -c conf/ -f job/mysink.conf -n a1 -Dflume.root.logger=INFO,console

nc localhost 44444
```

