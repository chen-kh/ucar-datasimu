package buaa.act.ucar.datasimu.config;

import java.util.Properties;

import kafka.producer.ProducerConfig;

public class KfkProducerConfig {
	private KfkProducerConfig() {
	}

	public static ProducerConfig getKfkProducerConfig() {
		try {
			Properties props = new Properties();
//			props.put("zookeeper.connect", CommonConfig.ZKADD);
			props.put("metadata.broker.list", CommonConfig.BROKER_LIST);// 如192.168.6.127:9092,192.168.6.128:9092
			/*
			 * request.required.acks 0, which means that the producer never
			 * waits for an acknowledgement from the broker (the same behavior
			 * as 0.7). This option provides the lowest latency but the weakest
			 * durability guarantees (some data will be lost when a server
			 * fails). 1, which means that the producer gets an acknowledgement
			 * after the leader replica has received the data. This option
			 * provides better durability as the client waits until the server
			 * acknowledges the request as successful (only messages that were
			 * written to the now-dead leader but not yet replicated will be
			 * lost). -1, which means that the producer gets an acknowledgement
			 * after all in-sync replicas have received the data. This option
			 * provides the best durability, we guarantee that no messages will
			 * be lost as long as at least one in sync replica remains.
			 */
			props.put("request.required.acks", "-1");// 配置"0""1""-1"逐渐增强，0表示不需要任何ack，-1表示需要所有ISR
														// ack
			props.put("consumer.id", "null");// Generated automatically if not
												// set.
			props.put("socket.timeout.ms", "30*1000");// Generated automatically
														// if
														// not set.
//			props.put("serializer.class", "kafka.serializer.StringEncoder");// 配置value的序列化类
			// 配置key的序列化类
			props.put("key.serializer.class", "kafka.serializer.StringEncoder");
			props.put("partitioner.class", "buaa.act.ucar.datasimu.kfk.KFKPartitioner");
			/** 异步发送配置 */
			if (CommonConfig.KfkProdAsync == true) {
				props.put("producer.type", "async");
				props.put("queue.buffering.max.ms", "5000");// 默认值5000
				props.put("queue.buffering.max.messages", "10000");// 默认值10000
				props.put("queue.enqueue.timeout.ms", "-1");// 0表示超过阻塞时长之后抛弃，-1表示无阻塞超时限制
				props.put("batch.num.messages", "5000");// 一个batch发送的数据量
			}
			return new ProducerConfig(props);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
