package io.lany.transactiontest;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;


import java.util.*;

/**
 * https://www.baeldung.com/kafka-exactly-once
 * @description: ReadProcessWrite 模式 将读和写封装在一个事务中
 * @author: lany
 * @date: Created in 2020/7/22 12:23
 */
public class ReadProcessWrite {

    private static final String KAFKA_READ_TOPIC = "testTwo";
    private static final String KAFKA_WRITE_TOPIC = "testThree";
    private static final String KAFKA_CONSUMER_GROUP_ID = "Local_Consumer";

    public static void main(String[] args) {
        // 消费者
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.188.1:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,KAFKA_CONSUMER_GROUP_ID);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        consumerProps.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        // 实例化一个consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer(consumerProps);
        // 订阅topic
        consumer.subscribe(Collections.singleton(KAFKA_READ_TOPIC));

        Properties producerProps= new Properties();
        // kafka 地址
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.188.1:9092");
        // 序列化
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // 设置事务id，表示开启事务机制
        producerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction2-read-process-write");
        // 设置事务时间 最大15min
        producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(900000));
        // ack 应答机制，保证数据可靠性
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        // 实例化一个producer
        KafkaProducer<String,String> producer = new KafkaProducer(producerProps);

        producer.initTransactions();
       while(true){
           ConsumerRecords<String,String> records = consumer.poll(2000);
           producer.beginTransaction();
           try {
               records.forEach(record -> {
                   String value = record.value();
                   System.out.print("CurrentOffset : "+ record.offset()+" ");
                   producer.send(new ProducerRecord<>(KAFKA_WRITE_TOPIC, "Trans ->" + value));
                   // https://stackoverflow.com/questions/45195010/meaning-of-sendoffsetstotransaction-in-kafka-0-11

               });
               producer.sendOffsetsToTransaction(currentOffsets(records),KAFKA_CONSUMER_GROUP_ID);
               producer.commitTransaction();
           } catch (Exception e) {
               e.printStackTrace();
               producer.abortTransaction();
           }
       }

    }

    /**
     * 获取当前offset
     * @param records
     * @return
     */
    private static Map<TopicPartition,OffsetAndMetadata> currentOffsets(ConsumerRecords<String,String> records){
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            System.out.println("Topic :"+partition.topic() +" - Offset:"+(offset+1));
            // 在提交offset的时候应该将offset+1在进行提交，这代表下次是从offset+1进行获取数据
            // 如果提交的是offset，那么在下次程序重新启动的时候会重新将Offset的位置消费一遍
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset+1));
        }
        return offsetsToCommit;
    }

}
