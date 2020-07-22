package io.lany.apitest;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * @description: Consumer Api测试
 * @author: lany
 * @date: Created in 2020/7/22 10:34
 */
public class Consumer {

    private static final String KAFKA_TOPIC = "testTwo";
    private static final String KAFKA_CONSUMER_ID = "consumer-id";

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.188.1:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,KAFKA_CONSUMER_ID);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        // 是否自动提交 -现在是手动提交
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        // 如果是自动提交，最好配置自动提交的时间 -现在配置的是500ms提交一次
//        consumerProps.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,String.valueOf(500));
        // 是否重置OFFSET
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 实例化一个consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer(consumerProps);
        // 订阅topic
        consumer.subscribe(Collections.singleton(KAFKA_TOPIC));

        while(true){
            // 每2s拉一次数据
            ConsumerRecords<String,String> records = consumer.poll(2000);
            records.forEach(record -> {
                System.out.println("Topic : "+ record.topic() + " Offset : " + record.offset() + " Value : "+ record.value());
            });
            // 设置同步提交 ，此时线程一直会堵塞，直到提交成功
//            consumer.commitSync();
            // 设置带有回调函数的异步提交
            consumer.commitAsync((map, e) -> {
                if( e == null){
                    map.forEach((topicPartition, offsetAndMetadata) -> {
//                        offsetAndMetadata.offset();
                        System.out.println("Partition : "+ topicPartition.partition() + " --- Topic :"+ topicPartition.topic());
                    });
                }else{
                    e.printStackTrace();
                }
            });
        }
    }
}
