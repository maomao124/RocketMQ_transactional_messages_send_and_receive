package mao.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

/**
 * Project name(项目名称)：RocketMQ_事务消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): TransactionalProducer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/10
 * Time(创建时间)： 15:53
 * Version(版本): 1.0
 * Description(描述)： 生产者-事务消息
 */

public class TransactionalProducer
{
    /**
     * 标签
     */
    private static final String[] tags = new String[]{"TagA", "TagB", "TagC"};

    public static void main(String[] args) throws MQClientException, InterruptedException
    {
        //生产者
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer("mao_group");
        //事务监听器
        TransactionListener transactionListener = new TransactionListenerImpl();
        //设置nameserver地址
        transactionMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //设置监听器
        transactionMQProducer.setTransactionListener(transactionListener);
        //启动生产者
        transactionMQProducer.start();
        //发送30条消息
        for (int i = 0; i < 30; i++)
        {
            try
            {
                //标签
                String tag = tags[i % (tags.length)];
                //消息体
                String body = "标签：" + tag + "   消息：" + i;
                //消息对象
                Message message = new Message("test_topic", tag, body.getBytes(StandardCharsets.UTF_8));
                //发送事务消息
                TransactionSendResult transactionSendResult = transactionMQProducer.sendMessageInTransaction(message, null);
                Thread.sleep(500);
                //打印
                System.out.println(body);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
