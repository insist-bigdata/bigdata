package top.damoncai.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author zhishun.cai
 * @date 2021/3/19 17:16
 */

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
