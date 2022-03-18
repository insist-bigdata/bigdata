package top.damoncai.rtdw.log.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/2/24 20:37
 */
@RestController
@Slf4j
public class LoggerController {


    //Spring提供的对Kafka的支持
    @Autowired  //  将KafkaTemplate注入到Controller中
            KafkaTemplate kafkaTemplate;

    //http://localhost:8080/applog

    //提供一个方法，处理模拟器生成的数据
    //@RequestMapping("/applog")  把applog请求，交给方法进行处理
    //@RequestBody   表示从请求体中获取数据
    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog){
        //System.out.println(mockLog);
        //落盘
        log.info(mockLog);
        //根据日志的类型，发送到kafka的不同主题中去
        //将接收到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if(startJson != null){
            //启动日志
            kafkaTemplate.send("gmall_start_0523",mockLog);
        }else{
            //事件日志
            kafkaTemplate.send("gmall_event_0523",mockLog);
        }
        return "success";
    }
}
