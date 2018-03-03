package buaa.act.ucar.datasimu.kfk;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.zuche.us.thrift.ThriftObdGps;

import buaa.act.ucar.datasimu.config.KfkProducerConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.sf.json.JSONObject;

public class GpsProducer extends Thread {
	private Producer<String, byte[]> producer;
	private String topic;
	private static final String KeyNumTimestamp = "0";
	private static final String KeyNumDevicesn = "1";
	private static final String KeyNumLatitude = "2";
	private static final String KeyNumLongitude = "3";
	private static final String KeyNumSpeed = "4";
	private static final String KeyNumDirection = "5";
	private static final String KeyNumAccuracy = "6";
	private static final String KeyNumHeight = "7";
	private static final String KeyNumPositionMode = "8";
	private LinkedBlockingQueue<String> gpsMessagesQueue = new LinkedBlockingQueue<String>(10 * 1000);

	public GpsProducer(String topic) {
//		Properties props = new Properties();
//		props.put("zookeeper.connect", KafkaProperties.ZK);
//		props.put("metadata.broker.list", KafkaProperties.BROKER_LIST);// 如192.168.6.127:9092,192.168.6.128:9092
//		props.put("request.required.acks", "-1");
//		props.put("consumer.id", "null");// Generated automatically if not set.
//		props.put("socket.timeout.ms", "30*1000");// Generated automatically if
//													// not set.
//		// props.put("serializer.class","kafka.serializer.StringEncoder");//配置value的序列化类
//		props.put("key.serializer.class", "kafka.serializer.StringEncoder");// 配置key的序列化类
//		/** 异步发送配置 */
//		props.put("producer.type", "async");
//		props.put("queue.buffering.max.ms", "5000");// 默认值5000
//		props.put("queue.buffering.max.messages", "10000");// 默认值10000
//		props.put("queue.enqueue.timeout.ms", "-1");// 0表示超过阻塞时长之后抛弃，-1表示无阻塞超时限制
//		props.put("batch.num.messages", "500");// 一个batch发送的数据量
//		producer = new Producer<String, byte[]>(new ProducerConfig(props));
		producer = new Producer<String, byte[]>(KfkProducerConfig.getKfkProducerConfig());
		this.topic = topic;
	}

	public void run() {
		TSerializer ts = new TSerializer(new TCompactProtocol.Factory());
		long offsetNo = 0;
		// 需要序列化的类
		ThriftObdGps gps = new ThriftObdGps();
		String sn = null; // required
		long timestamp; // required
		double lon; // optional
		double lat; // optional
		int accuracy; // optional
		double speed; // optional
		int direction; // optional
		double height; // optional
		int positionMode; // optional
		height = speed = lon = lat = -1.0;
		timestamp = positionMode = direction = accuracy = -1;// 设定默认值
		while (true) {
			try {
				String message = gpsMessagesQueue.take();
				JSONObject res = JSONObject.fromObject(message);
				if (res.containsKey(KeyNumDevicesn))
					sn = res.getString(KeyNumDevicesn);
				if (res.containsKey(KeyNumTimestamp))
					timestamp = res.getLong(KeyNumTimestamp);
				if (res.containsKey(KeyNumSpeed))
					speed = res.getDouble(KeyNumSpeed);
				if (res.containsKey(KeyNumLatitude))
					lat = res.getDouble(KeyNumLatitude);
				if (res.containsKey(KeyNumLongitude))
					lon = res.getDouble(KeyNumLongitude);
				if (res.containsKey(KeyNumAccuracy))
					accuracy = res.getInt(KeyNumAccuracy);
				if (res.containsKey(KeyNumHeight))
					height = res.getDouble(KeyNumHeight);
				if (res.containsKey(KeyNumPositionMode))
					positionMode = res.getInt(KeyNumPositionMode);
				if (res.containsKey(KeyNumDirection))
					direction = res.getInt(KeyNumDirection);
				gps.setSn(sn);
				gps.setGpstime(timestamp);
				gps.setSpeed(speed);
				gps.setAccuracy(accuracy);
				gps.setHeight(height);
				gps.setLat(lat);
				gps.setLon(lon);
				gps.setDirection(direction);
				gps.setPositionMode(positionMode);
				if (gps.getSn() != null) {
					try {
						producer.send(new KeyedMessage<String, byte[]>(topic, sn, ts.serialize(gps)));
						 if (offsetNo % 500 == 0) {
							System.out.println("send message_" + offsetNo + " : " + gps.toString());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				offsetNo++;
			} catch (Exception ex) {
				ex.printStackTrace();
				producer.close();
			}
		}
	}

	public void tryAddGpsMessage(String msg) {
		try {
			gpsMessagesQueue.put(msg);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/*
	 * class GpsMessagesMap { private int timeId; private List<String>
	 * messagesList;
	 * 
	 * public GpsMessagesMap(int timeId, List<String> messagesList) {
	 * this.timeId = timeId; this.messagesList = messagesList; }
	 * 
	 * public int getTimeId() { return timeId; }
	 * 
	 * public List<String> getMessagesList() { return messagesList; }
	 * 
	 * public void setTimeId(int timeId) { this.timeId = timeId; }
	 * 
	 * public void setMessagesList(List<String> list) { this.messagesList =
	 * list; } }
	 */
}
