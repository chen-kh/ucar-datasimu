package buaa.act.ucar.datasimu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import buaa.act.ucar.datasimu.config.HBaseConfig;

public class HBaseScanner {

	private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
	// private static final byte[] OBDCF = Bytes.toBytes("O");
	private static final byte[] GPSCF = Bytes.toBytes("G");
	private static final byte[] RAWCF = Bytes.toBytes("R");// 列族

	// private static Configuration conf;
	private Connection connection;
	private static Connection[] conns;
	private static AtomicInteger conn_num = new AtomicInteger(0);
	private HTable table = null;// HTable 线程不安全不要使用静态
	private static Map<String, byte[]> columns;

	static {
		columns = new HashMap<String, byte[]>();
		columns.put("longitude", Bytes.toBytes("JD"));
		columns.put("latitude", Bytes.toBytes("WD"));
		columns.put("direction", Bytes.toBytes("D"));
		columns.put("speed", Bytes.toBytes("S"));
		columns.put("totalfuel", Bytes.toBytes("TF"));
		columns.put("totalmileage", Bytes.toBytes("TM"));
		columns.put("mileage", Bytes.toBytes("M"));
		columns.put("OBDspeed", Bytes.toBytes("S"));
		columns.put("enginespeed", Bytes.toBytes("ES"));
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", HBaseConfig.Quorum);
		customConf.setLong("hbase.rpc.timeout", Integer.parseInt(HBaseConfig.Timeout));
		customConf.setLong("hbase.client.scanner.caching", Integer.parseInt(HBaseConfig.Caching));
		customConf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			/*
			 * HTable的的父类HTableInterface是非线程安全的，
			 * 针对每个线程建议都应该创建一个新的HTableInterface实例，这个创建过程是轻量的，
			 * 缓存HTableInterface对象既不是必须的也不是推荐的！
			 * 然后再联想到只要HTable使用的Configuration是同一个，那么它们一定是共用一个HConnection的，
			 * HConnection才是HBase客户端到Hbase集群的真正的连接。再想想HTablePool的作用，
			 * 无非就是HTable的连接池，里面维护的HTable应该来说都是使用的同一个HConnecion。
			 */
			conns = new Connection[5];
			for (int i = 0; i < 5; i++) {
				conns[i] = ConnectionFactory.createConnection(customConf);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public HBaseScanner() {
		connection = conns[conn_num.getAndIncrement() % 5];
		try {
			table = (HTable) connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public SortedMap<Long, List<String>> scanGPS(long startTime, long stopTime, String[] devicesnList) {
		final String KeyNumTimestamp = "0";
		final String KeyNumDevicesn = "1";
		final String KeyNumLatitude = "2";
		final String KeyNumLongitude = "3";
		final String KeyNumSpeed = "4";
		final String KeyNumDirection = "5";
		SortedMap<Long, List<String>> gpsMapKeyedByTimestamp = new TreeMap<Long, List<String>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
		for (String devicesn : devicesnList) {
			try {
				byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
				byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);
				Scan scanner = new Scan();
				scanner.addFamily(GPSCF);
				scanner.addColumn(GPSCF, columns.get("longitude"));
				scanner.addColumn(GPSCF, columns.get("latitude"));
				scanner.addColumn(GPSCF, columns.get("direction"));
				scanner.addColumn(GPSCF, columns.get("speed"));
				scanner.setStartRow(startRow);
				scanner.setStopRow(stopRow);
				scanner.setReversed(true);
				ResultScanner resultScanner = table.getScanner(scanner);
				for (Result res : resultScanner) {
					if (res != null) {
						Double longitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("longitude")));
						Double latitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("latitude")));
						Integer direction = Bytes.toInt(res.getValue(GPSCF, columns.get("direction")));
						Double speed = Bytes.toDouble(res.getValue(GPSCF, columns.get("speed")));
						String rk = Bytes.toString(res.getRow());
						long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));
						JSONObject object = new JSONObject();
						object.put(KeyNumTimestamp, timestamp);
						object.put(KeyNumDevicesn, devicesn);
						object.put(KeyNumLatitude, latitude);
						object.put(KeyNumLongitude, longitude);
						object.put(KeyNumDirection, direction);
						object.put(KeyNumSpeed, speed);
						addGpsMessage(timestamp, object.toString(), gpsMapKeyedByTimestamp);
					}
				}
				resultScanner.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return gpsMapKeyedByTimestamp;
	}

	// 获取排好序的obd的原始信息
	public SortedMap<Long, List<String>> scanRawOBD(long startTime, long stopTime, String[] devicesnList) {
		final String KeyGpstime = "gpstime";
		SortedMap<Long, List<String>> rawMapKeyedByTimestamp = new TreeMap<Long, List<String>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
		for (String devicesn : devicesnList) {
			try {
				byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
				byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);
				Scan scanner = new Scan();
				scanner.addFamily(RAWCF);
				scanner.setStartRow(startRow);
				scanner.setStopRow(stopRow);
				scanner.setReversed(true);
				ResultScanner resultScanner = table.getScanner(scanner);
				for (Result res : resultScanner) {
					if (res != null) {
						String raw = Bytes.toString(res.getValue(RAWCF, RAWCF));
						long gpstime = new JSONObject(raw).getLong(KeyGpstime);
						addRawMessage(gpstime, raw, rawMapKeyedByTimestamp);
					}
				}
				resultScanner.close();
			} catch (Exception e) {
				e.printStackTrace();;
			}
		}
		return rawMapKeyedByTimestamp;
	}

	// 获取没有排好序的obd的原始信息
	public List<String> scanRawOBD2getMsgList(long startTime, long stopTime, String[] devicesnList) {
		List<String> resList = new ArrayList<String>();
		for (String devicesn : devicesnList) {
			try {
				byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
				byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);
				Scan scanner = new Scan();
				scanner.addFamily(RAWCF);
				scanner.setCaching(100);
				scanner.setStartRow(startRow);
				scanner.setStopRow(stopRow);
				scanner.setReversed(true);
				ResultScanner resultScanner = table.getScanner(scanner);
				for (Result res : resultScanner) {
					if (res != null) {
						String raw = Bytes.toString(res.getValue(RAWCF, RAWCF));
						resList.add(raw);
					}
				}
				resultScanner.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return resList;
	}

	// 获取没有排好序的gps的原始信息
	public List<String> scanGPS2getMsgList(long startTime, long stopTime, String[] devicesnList) {
		List<String> resList = new ArrayList<String>();
		final String KeyNumTimestamp = "0";
		final String KeyNumDevicesn = "1";
		final String KeyNumLatitude = "2";
		final String KeyNumLongitude = "3";
		final String KeyNumSpeed = "4";
		final String KeyNumDirection = "5";
		for (String devicesn : devicesnList) {
			try {
				byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
				byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);
				Scan scanner = new Scan();
				scanner.addFamily(GPSCF);
				scanner.addColumn(GPSCF, columns.get("longitude"));
				scanner.addColumn(GPSCF, columns.get("latitude"));
				scanner.addColumn(GPSCF, columns.get("direction"));
				scanner.addColumn(GPSCF, columns.get("speed"));
				scanner.setCaching(100);
				scanner.setStartRow(startRow);
				scanner.setStopRow(stopRow);
				scanner.setReversed(true);
				ResultScanner resultScanner = table.getScanner(scanner);
				for (Result res : resultScanner) {
					if (res != null) {
						Double longitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("longitude")));
						Double latitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("latitude")));
						Integer direction = Bytes.toInt(res.getValue(GPSCF, columns.get("direction")));
						Double speed = Bytes.toDouble(res.getValue(GPSCF, columns.get("speed")));
						String rk = Bytes.toString(res.getRow());
						long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));
						JSONObject object = new JSONObject();
						object.put(KeyNumTimestamp, timestamp);
						object.put(KeyNumDevicesn, devicesn);
						object.put(KeyNumLatitude, latitude);
						object.put(KeyNumLongitude, longitude);
						object.put(KeyNumDirection, direction);
						object.put(KeyNumSpeed, speed);
						resList.add(object.toString());
					}
				}
				resultScanner.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return resList;
	}

	private void addRawMessage(long timestamp, String raw, SortedMap<Long, List<String>> rawMapKeyedByTimestamp) {
		List<String> obdMessageList = rawMapKeyedByTimestamp.getOrDefault(timestamp, new ArrayList<String>());
		obdMessageList.add(raw);
		rawMapKeyedByTimestamp.put(timestamp, obdMessageList);
	}

	private void addGpsMessage(Long timestamp, final String gpsMessage,
			SortedMap<Long, List<String>> gpsMapKeyedByTimestamp) {
		List<String> gpsMessageList = gpsMapKeyedByTimestamp.getOrDefault(timestamp, new ArrayList<String>());
		gpsMessageList.add(gpsMessage);
		gpsMapKeyedByTimestamp.put(timestamp, gpsMessageList);
	}

	public void closeHTable() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
