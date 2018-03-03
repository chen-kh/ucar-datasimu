import java.awt.Container;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import buaa.act.ucar.datasimu.Simulator;
import buaa.act.ucar.datasimu.hbase.Utils;
import buaa.act.ucar.datasimu.zk.ZkClient;

public class DeleteData {
	private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
	// private static final byte[] OBDCF = Bytes.toBytes("O");
	private static final byte[] GPSCF = Bytes.toBytes("G");
	private static final byte[] RAWCF = Bytes.toBytes("R");// 列族
	private Connection connection;
	private static HTable table = null;
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
	}

	public DeleteData() {
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", "192.168.7.70");
		customConf.setLong("hbase.rpc.timeout", 600000);
		customConf.setLong("hbase.client.scanner.caching", 1000);
		customConf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			connection = ConnectionFactory.createConnection(customConf);
			try {
				table = (HTable) connection.getTable(TableName.valueOf(TABLE_NAME));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		deleteAllGpsFiles(getRealPath() + "/created/gps");
		deleteAllObdFiles(getRealPath() + "/created/obd");
		deleteAllGpsFiles(getRealPath() + "/mixed1/gps");
		deleteAllObdFiles(getRealPath() + "/mixed1/obd");
		deleteAllGpsFiles(getRealPath() + "/mixed2/gps");
		deleteAllObdFiles(getRealPath() + "/mixed2/obd");
	}

	public void deleteGPS(long startTime, long stopTime, String devicesn) {
		try {
			byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
			byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);
			Scan scanner = new Scan();
			scanner.setStartRow(startRow);
			scanner.setStopRow(stopRow);
			scanner.setReversed(true);
			ResultScanner resultScanner = table.getScanner(scanner);
			for (Result res : resultScanner) {
				if (res != null) {
					String rk = Bytes.toString(res.getRow());
					// System.out.println("rk for gps = " + rk);
					long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));
					Delete delete = new Delete(Utils.generateRowkeyPM(timestamp, devicesn));
					table.delete(delete);
				}
			}
			resultScanner.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 获取排好序的obd的原始信息
	public void deleteOBD(long startTime, long stopTime, String devicesn) {
		final String KeyGpstime = "gpstime";
		final String KeyDevicesn = "devicesn";
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
					JSONObject jo = new JSONObject(raw);
					// System.out.println(jo);
					long gpstime = jo.getLong(KeyGpstime);
					Delete delete = new Delete(Utils.generateRowkeyPM(gpstime, devicesn));
					table.delete(delete);
				}
			}
			resultScanner.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void deleteAllGpsFiles(String path) {
		String[] files = new File(path).list();
		for (String file : files) {
			new File(path + "/" + file).delete();
		}
	}

	private static void deleteAllObdFiles(String path) {
		String[] files = new File(path).list();
		for (String file : files) {
			new File(path + "/" + file).delete();
		}
	}

	private static String getRealPath() {
		String rootPath = Simulator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}
}
