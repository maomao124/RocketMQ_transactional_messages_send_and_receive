package mao.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Project name(项目名称)：RocketMQ_事务消息的发送与接收
 * Package(包名): mao.consumer
 * Class(类名): Consumer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/10
 * Time(创建时间)： 16:11
 * Version(版本): 1.0
 * Description(描述)： 消息消费者
 */

public class Consumer
{
    public static void main(String[] args) throws MQClientException
    {
        //消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("mao_group");
        //nameserver
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        //订阅
        defaultMQPushConsumer.subscribe("test_topic", "*");
        //监听器
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently()
        {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext)
            {
                for (MessageExt messageExt : list)
                {
                    System.out.println(new String(messageExt.getBody(), StandardCharsets.UTF_8));
                }
                //返回成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动
        defaultMQPushConsumer.start();
    }
}
