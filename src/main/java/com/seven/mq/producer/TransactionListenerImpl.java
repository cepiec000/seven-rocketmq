package com.seven.mq.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @Description: TODO
 * @Author chendongdong
 * @Date 2020/4/14 18:07
 * @Version V1.0
 **/
public class TransactionListenerImpl implements TransactionListener {
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
        System.out.println("====executeLocalTransaction=======");

        String body = new String(message.getBody());
        String key = message.getKeys();
        String transactionId = message.getTransactionId();
        System.out.println("transactionId="+transactionId+", key="+key+", body="+body);
        // 执行本地事务begin TODO
        System.out.println("开始执行本地事物，本地消息成功，则提交二次确认 COMMIT_MESSAGE");
        // 执行本地事务end TODO

        int status = Integer.parseInt(arg.toString());

        //二次确认消息，然后消费者可以消费
        if(status == 1){
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        //回滚消息，broker端会删除半消息
        if(status == 2){
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        //broker端会进行回查消息，再或者什么都不响应
        if(status == 3){
            return LocalTransactionState.UNKNOW;
        }

        return null;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("====checkLocalTransaction=======");
        String body = new String(messageExt.getBody());
        String key = messageExt.getKeys();
        String transactionId = messageExt.getTransactionId();
        System.out.println("transactionId="+transactionId+", key="+key+", body="+body);
        //要么commit 要么rollback
        System.out.println("检查本地事物是否成功，用于二次提交失败后，Mq服务回查半消息，状态");
        //TODO 检查本地是否完成 完成返回 COMMIT_MESSAGE 否则返回 ROLLBACK_MESSAGE
        //可以根据key去检查本地事务消息是否完成
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
