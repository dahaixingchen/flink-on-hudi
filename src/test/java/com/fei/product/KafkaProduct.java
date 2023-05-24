package com.fei.product;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * @Description:
 * @ClassName: KafkaProduct
 * @Author chengfei
 * @DateTime 2023/5/24 10:33
 **/
public class KafkaProduct {
    public static void main(String[] args) {
        Properties pro = new Properties();
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.191.80.158:8092");
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer producer = new KafkaProducer(pro);


        for (int i = 0; i < 10; i++) {
            String uuid = UUID.randomUUID().toString().replace("-", "").substring(0,11);
            ProducerRecord<String, String> record = new ProducerRecord<>("test_tp", uuid + ",zs,10," + System.currentTimeMillis() + ",2023-05-0" + i);
            producer.send(record);
            System.out.println("数据发送成功");
            System.out.println(record);
        }

        producer.flush();

    }
}
