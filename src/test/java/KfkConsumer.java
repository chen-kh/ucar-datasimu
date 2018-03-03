

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.zuche.us.thrift.ThriftObdDs;
import com.zuche.us.thrift.ThriftObdGps;

import buaa.act.ucar.datasimu.config.CommonConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KfkConsumer extends Thread {
	private ConsumerConnector consumer;
	private String topic;
//	private static BufferedWriter writer;
	private static TDeserializer tDeserializer = new TDeserializer(new TCompactProtocol.Factory());;
	private static ThriftObdDs thriftObd = new ThriftObdDs();
	private static ThriftObdGps thriftgps = new ThriftObdGps();
	public KfkConsumer(String topic) {
		consumer = Consumer.createJavaConsumerConnector(this.consumerConfig());
		this.topic = topic;
	}

	private ConsumerConfig consumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", CommonConfig.ZKADD);
		props.put("group.id", CommonConfig.GROUP_ID);
		props.put("group.id", "tjt_1");
		props.put("auto.commit.enable", "false");// 默认为true，让consumer定期commit
													// offset，zookeeper会将offset写入到文件中，否则只在内存，若故障则再消费时会重头开始
		props.put("auto.offset.reset", "smallest");// what to do if an offset is
													// out of range.默认为largest
		props.put("auto.commit.interval.ms", CommonConfig.INTERVAL + "");
		props.put("zookeeper.session.timeout.ms", CommonConfig.TIMEOUT + "");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("rebalance.backoff.ms", "2000");
		props.put("rebalance.max.retries", "10");
		return new ConsumerConfig(props);
	}

	@Override
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println(Thread.currentThread().getName());
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> mam = it.next();
			try {
				tDeserializer.deserialize(thriftgps, mam.message());
			} catch (TException e1) {
				e1.printStackTrace();
			}
			try {
				System.out.println(
						"partition[" + mam.partition() + "]," + "offset[" + mam.offset() + "], " + thriftObd.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			/*try {
				sleep(1000);
			} catch (Exception ex) {
				ex.printStackTrace();
			}*/
		}
	}
	public static void main(String[] args) {
		new KfkConsumer("PlayGps").start();
	}
}
