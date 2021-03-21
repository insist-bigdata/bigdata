# 自定义UDF函数

### 需求

**自定义UDF实现计算给定字符串的长度**

```shell
hive(default)> select my_len("abcd");

4
```



1. 创建Maven工程

2. 导入依赖

   ```xml
   <dependencies>
       <dependency>
       <groupId>org.apache.hive</groupId>
       <artifactId>hive-exec</artifactId>
       <version>3.1.2</version>
       </dependency>
   </dependencies>
   ```

3. 创建类实现相关接口并编写逻辑

   ```java
   public class MyStringLength extends GenericUDF {
   
       /**
        *
        * @param arguments 输入参数类型的鉴别器对象
        * @return 返回值类型的鉴别器对象
        * @throws UDFArgumentException
        */
       public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
           // 判断输入参数的个数
           if(arguments.length !=1){
               throw new UDFArgumentLengthException("Input Args Length Error!!!");
           }
           // 判断输入参数的类型
   
           if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
           ){
               throw new UDFArgumentTypeException(0,"Input Args Type Error!!!");
           }
           //函数本身返回值为 int，需要返回 int 类型的鉴别器对象
           return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
       }
   
       public Object evaluate(DeferredObject[] arguments) throws HiveException {
           if(arguments[0].get() == null){
               return 0;
           }
           return arguments[0].get().toString().length();
       }
   
       /**
        * 用于编写执行计划
        * @param strings
        * @return
        */
       public String getDisplayString(String[] strings) {
           return "";
       }
   }
   ```

4. 打jar包上传到服务器

5. 将jar包添加到hive的classpath

   ```shell
   hive (default)> add jar /opt/module/data/myudf.jar;
   ```

6. 创建临时函数并与开发好的class关联

   ```shell
   hive (default)> create temporary function my_len as "com.atguigu.hive.MyStringLength";
   ```

7. 使用

   ```shell
   hive (default)> select ename,my_len(ename) ename_len from emp;
   ```



# 自定义UDTF函数

### 业务逻辑编写

```java
package top.damoncai.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhishun.cai
 * @date 2021/3/19 17:53
 */

public class MyUDTF extends GenericUDTF {

    private ArrayList<String> outList = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1.定义输出数据的列名和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        //2.添加输出数据的列名和类型
        fieldNames.add("lineToWord");

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //1.获取原始数据
        String arg = args[0].toString();
        //2.获取数据传入的第二个参数，此处为分隔符
        String splitKey = args[1].toString();
        //3.将原始数据按照传入的分隔符进行切分
        String[] fields = arg.split(splitKey);
        //4.遍历切分后的结果，并写出
        for (String field : fields) {
            //集合为复用的，首先清空集合
            outList.clear();
            //将每一个单词添加至集合
            outList.add(field);
            //将集合内容写出
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
```







