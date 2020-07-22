package io.lany.apitest;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @description: Producer API 测试
 * @author: lany
 * @date: Created in 2020/7/22 15:37
 */
public class Producer {

    private static final String KAFKA_TOPIC = "testThree";

    public static void main(String[] args) {
        Properties props = new Properties();

        // kafka 地址
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.188.1:9092");

        // 序列化
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // ack 应答机制，保证数据可靠性
        props.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        // 如果需要自定义分区器，需要在这里配置
//        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,"io.lany.custom.CustomPartition");
        // 自定义拦截器
        props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"io.lany.custom.CustomInterceptor");
        KafkaProducer producer = new KafkaProducer(props);
        String value;
        for(int i = 0; i < 30000; i++){
            value = "{\"num\" :\""+i+"\"}";
            // 只带value的发送
            producer.send(new ProducerRecord(KAFKA_TOPIC,value));
            // 带value的发送
//            producer.send(new ProducerRecord(KAFKA_TOPIC,"Key",value));
            // 带回调函数的发送
//            producer.send(new ProducerRecord(KAFKA_TOPIC, "Key", value), (recordMetadata, e) -> {
//                if(e != null){
//                    System.out.println("Partition : "+recordMetadata.partition() +"Offset :" + recordMetadata.offset());
//                }
//            });

            // 同步发送消息，是否调用get()方法
//            try {
//                producer.send(new ProducerRecord(KAFKA_TOPIC,value)).get();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
        }
        producer.close();

    }

}
