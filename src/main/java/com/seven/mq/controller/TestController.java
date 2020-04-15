package com.seven.mq.controller;

import com.seven.mq.config.MqConfig;
import com.seven.mq.producer.Producer;
import com.seven.mq.producer.TransactionProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: TODO
 * @Author chendongdong
 * @Date 2020/4/14 17:36
 * @Version V1.0
 **/
@Slf4j
@RestController
public class TestController {
    @Autowired
    private Producer producer;

    @Autowired
    private TransactionProducer transactionProducer;

    private List<String> mesList;

    /**
     * 初始化消息
     */
    public TestController() {
        mesList = new ArrayList<>();
        mesList.add("小小");
        mesList.add("爸爸");
        mesList.add("妈妈");
        mesList.add("爷爷");
        mesList.add("奶奶");

    }

    /**
     * 消息同步发送 速度快  不容易丢消息
     * @return
     * @throws Exception
     */
    @RequestMapping("/text/rocketmq")
    public Object callback() throws Exception {
        //总共发送五次消息
        for (String s : mesList) {
            //创建生产信息
            Message message = new Message(MqConfig.TOPIC, "testtag", ("小小一家人的称谓:" + s).getBytes());
            //发送
            SendResult sendResult = producer.getProducer().send(message);
            log.info("输出生产者信息={}",sendResult);
        }
        return "成功";
    }

    /**
     * 异步发送消息 速度快 不容易丢消息
     * @return
     * @throws Exception
     */
    @RequestMapping("/text/async/rocketmq")
    public Object callbackAsync() throws Exception {
        //总共发送五次消息
        for (int i=0;i<mesList.size();i++) {
            final int index=i;
            String s=mesList.get(i);
            //创建生产信息
            Message message = new Message(MqConfig.TOPIC, "asyncTag", ("小小一家人的称谓:" + s).getBytes());
            //异步发送
            producer.getProducer().send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.printf("%-10d Exception %s %n", index, throwable);
                    throwable.printStackTrace();
                }
            });
        }
        return "成功";
    }

    /**
     * 单向发送消息，
     * 性能最高，不可靠（可能会丢消息）
     * 发送特点为发送方只负责发送消息，不等待服务器回应且没有回调函数触发，即只发送请求不等待应答()。 此方式发送消息的过程耗时非常短，一般在微秒级别。
     * @return
     * @throws Exception
     */
    @RequestMapping("/text/oneway/rocketmq")
    public Object callbackOneway() throws Exception {
        //总共发送五次消息
        for (int i=0;i<mesList.size();i++) {
            final int index=i;
            String s=mesList.get(i);
            //创建生产信息
            Message message = new Message(MqConfig.TOPIC, "onewayTag", ("小小一家人的称谓:" + s).getBytes());
            //异步发送
            producer.getProducer().sendOneway(message);
        }
        return "成功";
    }

    /**
     * 发送事物消息
     * @return
     * @throws Exception
     */
    @RequestMapping("/text/transcation/rocketmq")
    public Object callbackTranscation() throws Exception {
        //总共发送五次消息
        for (int i=0;i<mesList.size();i++) {
            final int index=i;
            String s=mesList.get(i);
            //创建生产信息
            Message message = new Message(MqConfig.TOPIC_TRANS, "transactionTag", ("小小一家人的称谓:" + s).getBytes());
            //事务发送
            SendResult  sendResult= transactionProducer.getProducer().sendMessageInTransaction(message,"orgs");
            System.out.printf("发送结果=%s, sendResult=%s \n", sendResult.getSendStatus(), sendResult.toString());
        }
        return "成功";
    }
}
