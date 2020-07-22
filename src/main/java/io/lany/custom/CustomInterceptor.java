package io.lany.custom;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @description: 自定义生产者拦截器
 * 实现发送数量的统计
 * @author: lany
 * @date: Created in 2020/7/22 16:16
 */
public class CustomInterceptor implements ProducerInterceptor {

    private int success = 0;
    private int fail = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null){
            success++;
        }else {
            fail++;
        }
    }

    @Override
    public void close() {
        System.out.println("Success : " + success +"Fail :" + fail);
    }

    @Override
    public void configure(Map<String, ?> map) {

        System.out.println("configure...");

    }
}
