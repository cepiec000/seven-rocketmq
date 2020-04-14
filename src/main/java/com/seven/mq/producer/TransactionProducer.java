package com.seven.mq.producer;

import com.seven.mq.config.MqConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * @Description: TODO
 * @Author chendongdong
 * @Date 2020/4/14 17:53
 * @Version V1.0
 **/
@Slf4j
@Component
public class TransactionProducer {
    private String producerGroup = "transaction_producer";
    private TransactionMQProducer producer;

    /**
     * 事物监听器
     */
    private TransactionListener transactionListener = new TransactionListenerImpl();


    /**
     * 一般自定义线程池的时候，需要给线程加个名称
     */
    private ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("client-transaction-msg-check-thread");
            return thread;
        }

    });

    public TransactionProducer(){
        //示例生产者
        producer = new TransactionMQProducer(producerGroup);
        //不开启vip通道 开通口端口会减2
        producer.setVipChannelEnabled(false);
        //绑定name server
        producer.setNamesrvAddr(MqConfig.NAME_SERVER);
        // 事务回查最小并发数
        producer.setTransactionListener(transactionListener);
        producer.setExecutorService(executorService);
        start();

    }
    /**
     * 对象在使用之前必须要调用一次，只能初始化一次
     */
    public void start(){
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public DefaultMQProducer getProducer(){
        return this.producer;
    }
    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown(){
        this.producer.shutdown();
    }




}
