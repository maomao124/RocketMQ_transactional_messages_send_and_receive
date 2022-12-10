package mao.producer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Project name(项目名称)：RocketMQ_事务消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): TransactionListenerImpl
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/10
 * Time(创建时间)： 16:01
 * Version(版本): 1.0
 * Description(描述)： 事务监听器实现
 */

public class TransactionListenerImpl implements TransactionListener
{
    /**
     * 执行本地事务
     *
     * @param msg 消息
     * @param arg 参数
     * @return {@link LocalTransactionState}
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg)
    {
        System.out.println("执行本地事务");
        if (StringUtils.equals("TagA", msg.getTags()))
        {
            //如果是标签a的消息，直接提交
            return LocalTransactionState.COMMIT_MESSAGE;
        }
        else if (StringUtils.equals("TagB", msg.getTags()))
        {
            //如果是标签B的消息，回滚
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        else
        {
            //否则，不知道，中间状态
            return LocalTransactionState.UNKNOW;
        }

    }

    /**
     * 检查本地事务
     *
     * @param msg 消息
     * @return {@link LocalTransactionState}
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg)
    {
        System.out.println("MQ检查消息Tag【" + msg.getTags() + "】的本地事务执行结果");
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
