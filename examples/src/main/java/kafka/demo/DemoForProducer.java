package kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DemoForProducer extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DemoForProducer.class);
    public static void main(String[] args) {
        System.out.println("hello3-11-3");
        log.debug("DemoForProducer hell");
        DemoForProducer dfp = new DemoForProducer("yuxh_DemoForProducer11-2", true);
        dfp.run();
    }

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public DemoForProducer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("client.id", "DemoForProducer3");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public DemoForProducer(KafkaProducer<Integer, String> producer, String topic, Boolean isAsync) {
        this.producer = producer;
        this.topic = topic;
        this.isAsync = isAsync;
    }

    // 线程类的执行方法（while死循环），判断是异步还是同步发送配置（高版本默认都异步），调用send方法发送数据,
    // send方法的第1个参数是ProducerRecord，第2个是messageNo记录发送批次，DemoCallBack是回执函数
    public void run() {
        int messageNo = 3;
        String messageStr = "Message_" + messageNo;
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            System.out.println("asynchronously send...");
            producer.send(new ProducerRecord<>(topic,
                    messageNo,
                    messageStr), new DemoForCallBack(startTime, messageNo, messageStr));

            producer.close();
            System.out.println("awake DemoForCallBack asynchronously end");
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr)).get();
                System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
//        while (true) {
//            String messageStr = "Message_" + messageNo;
//            long startTime = System.currentTimeMillis();
//            if (isAsync) { // Send asynchronously
//                producer.send(new ProducerRecord<>(topic,
//                        messageNo,
//                        messageStr), new DemoForCallBack(startTime, messageNo, messageStr));
//            } else { // Send synchronously
//                try {
//                    producer.send(new ProducerRecord<>(topic,
//                            messageNo,
//                            messageStr)).get();
//                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
//                } catch (InterruptedException | ExecutionException e) {
//                    e.printStackTrace();
//                }
//            }
//            ++messageNo;
//        }
    }

    class DemoForCallBack implements Callback {

        private final long startTime;
        private final int key;
        private final String message;

        public DemoForCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method
         * will be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            System.out.println("DemoForCallBack onCompletion metadata="+metadata);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println(
                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                                "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }

}

