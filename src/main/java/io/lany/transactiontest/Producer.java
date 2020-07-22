package io.lany.transactiontest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description: Write 模式 将发送的多条消息放到一个事务中
 * @author: lany
 * @date: Created in 2020/7/22 10:35
 */
public class Producer {

    private static final String KAFKA_TOPIC = "testOne";

    public static void main(String[] args) {
        Properties props = new Properties();

        // kafka 地址
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.188.1:9092");

        // 序列化
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // 设置事务id，表示开启事务机制
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction-test");
        // 设置事务时间 最大15min
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(900000));
        // ack 应答机制，保证数据可靠性
        props.setProperty(ProducerConfig.ACKS_CONFIG,"-1");

        KafkaProducer producer = new KafkaProducer(props);

        /** 1. init事务
         *  Producer会发送一个initpidrequest 给 Transaction Manager
         *  Transaction Manager在接收到请求后会将pid以及epoch给初始化
         *  Transaction Manager会根据tid设置事务超时时间
         *  Transaction Manager 在transaction log中记录<transactionid,pid>
         * 除此之外，还会恢复之前producer未完成的事务，以及对pid的epoch进行递增
         */
        producer.initTransactions();

        /**
         * 2. 开启一个事务
         * 该过程将事务的状态改变为in-transaction，
         */
        producer.beginTransaction();

        String value = "";
        try {
            for(int i = 0; i < 30000; i++){
                value = "{\"num\" :\""+i+"\"}";
                /**
                 * 3. 发送消息
                 */
                producer.send(new ProducerRecord(KAFKA_TOPIC,value));
                Thread.sleep(1000);
            }

            /**
             * 4. 提交事务
             * 在这里使用了一个2pc的提交方式
             *
             * 4.1 TransactionManager 会将事务状态设置为 COMMITTING_TRANSACTION
             * 4.2 将Transaction Marker写入到事务涉及的所有消息（将每条消息标记为committed或者aborted）
             * 一旦marker写入完成，那么manager会将最终的complete_commit或者complete_aborte状态写入transactionlog中
             * 标明事务完成
             */
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            producer.abortTransaction();
        }

    }

}
