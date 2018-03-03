package buaa.act.ucar.datasimu.kfk;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import com.zuche.us.thrift.ThriftObdDs;

import buaa.act.ucar.datasimu.config.KfkProducerConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.sf.json.JSONObject;

public class ObdProducer extends Thread {
	private Producer<String, byte[]> producer;
	private String topic;
	private static final String KeySpeed = "51";
	private static final String KeyTotalMile = "65";// key="65"对应总油耗
	private static final String KeyMile = "40";// key="65"对应mileage
	private static final String KeyTotalFuel = "42";
	private static final String KeyEngineSpeed = "52";
	private static final String KeyVin = "100";
	private static final String KeyDevicesn = "devicesn";
	private static final String KeyGpstime = "gpstime";
	public LinkedBlockingQueue<String> obdRawQueue = new LinkedBlockingQueue<String>(100 * 1000);

	public ObdProducer(String topic) {
//		Properties props = new Properties();
//		props.put("zookeeper.connect", KafkaProperties.ZK);
//		props.put("metadata.broker.list", KafkaProperties.BROKER_LIST);// 如192.168.6.127:9092,192.168.6.128:9092
//		props.put("request.required.acks", "-1");
//		props.put("consumer.id", "null");// Generated automatically if not set.
//		props.put("socket.timeout.ms", "30*1000");// Generated automatically if
//													// not set.
//		// props.put("serializer.class","kafka.serializer.StringEncoder");//配置value的序列化类
//		props.put("key.serializer.class", "kafka.serializer.StringEncoder");// 配置key的序列化类
//		props.put("partitioner.class", "buaa.act.ucar.datasimu.kfk.KFKPartitioner");
//		/** 异步发送配置 */
//		props.put("producer.type", "async");
//		props.put("queue.buffering.max.ms", "5000");// 默认值5000
//		props.put("queue.buffering.max.messages", "100000");// 默认值10000
//		props.put("queue.enqueue.timeout.ms", "-1");// 0表示超过阻塞时长之后抛弃，-1表示无阻塞超时限制
//		props.put("batch.num.messages", "5000");// 一个batch发送的数据量
//		producer = new Producer<String, byte[]>(new ProducerConfig(props));
		producer = new Producer<String, byte[]>(KfkProducerConfig.getKfkProducerConfig());
		this.topic = topic;
	}

	public void run() {
		TSerializer ts = new TSerializer(new TCompactProtocol.Factory());
		long offsetNo = 0;
		// 需要序列化的类
		ThriftObdDs ds = new ThriftObdDs();
		double total_mileage;
		double total_fuel;
		double mileage;
		double speed;
		double engine_speed;
		long gpstime = 0L;
		String devicesn = null;
		String vin;
		total_fuel = total_mileage = mileage = speed = engine_speed = -1;// 设定默认值
		vin = "";
		JSONObject res = new JSONObject();
		while (true) {
			try {
				String obdRaw = obdRawQueue.take();
				res = JSONObject.fromObject(obdRaw);
				if (res.containsKey(KeyTotalMile))
					total_mileage = res.getDouble(KeyTotalMile);
				if (res.containsKey(KeyMile))
					mileage = res.getDouble(KeyMile);
				if (res.containsKey(KeySpeed))
					speed = res.getDouble(KeySpeed);
				if (res.containsKey(KeyEngineSpeed))
					engine_speed = res.getDouble(KeyEngineSpeed);
				if (res.containsKey(KeyVin))
					vin = res.getString(KeyVin);
				if (res.containsKey(KeyDevicesn))
					devicesn = res.getString(KeyDevicesn);
				if (res.containsKey(KeyGpstime))
					gpstime = res.getLong(KeyGpstime);
				if (res.containsKey(KeyTotalFuel))
					total_fuel = res.getDouble(KeyTotalFuel);
				ds.setSn(devicesn);
				ds.setGpstime(gpstime);
				ds.setTotalFuel(total_fuel);
				ds.setTotalMileage(total_mileage);
				ds.setMileage(mileage);
				ds.setSpeed(speed);
				ds.setEngineSpeed(engine_speed);
				ds.setVin(vin);
				ds.setRes(res.toString());
				if (ds.getSn() != null) {
					try {
						producer.send(new KeyedMessage<String, byte[]>(topic, devicesn, ts.serialize(ds)));
						if (offsetNo % 500 == 0) {
							System.out.println("send message_" + offsetNo + " : " + ds.toString());
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

	public void tryAddObdMessage(String msg) {
		try {
			obdRawQueue.put(msg);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
