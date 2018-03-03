package buaa.act.ucar.datasimu.core2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

import buaa.act.ucar.datasimu.hbase.HBaseScanner;

/**
 * 类说明：数据产生器，原理是给定一个固定大小的devicesn列表，对列表中每一个devicesn去HBase扫描固定的时间段（以秒为单位），
 * 然后根据这次的扫描结果依次调整时间戳（依次延迟或提前几秒）得到多辆模拟车的数据。这个方法建议在车的数据量很大的时候采用，一般模拟数量大于500万时。
 * 原因在于，低于500万的时候，为了使得模拟的数据之间的相似性看上去很小，我们对一辆车进行不同时间段的扫描去得到相应的模拟车辆的数据。
 * 而当模拟的数据量很大的时候，扫描Hbase的工作将变得非常繁重，模拟车的数量与对HBase的扫描次数相等，我们退而求其次。当然，如果并不需要很长时间的数据
 * ，我们仍然可以用之前的方法。
 * <p>
 * 另外这个方法产生数据是按照每1800s的数据产生一次的，一旦模拟时间超过1800s，会导致两次数据不连续。
 * 
 * @author 00000000000000000000
 *
 */
public class DataGenerator4large extends Thread {
	private static Object lock = new Object();
	private String name;
	private Map<Integer, String[]> devGroupMap = new HashMap<Integer, String[]>();// 作为这个生成器持有的groupId->devList对儿
	private static final String KeyNumTimestamp = "0";
	private static final String KeyNumDevicesn = "1";
	private static final String KeyDevicesn = "devicesn";
	private static final String KeyGpstime = "gpstime";
	public static final long hourSeconds = 60 * 60L;
	public static final long daySeconds = 24 * hourSeconds;
	private String rootPath;// 写入的根目录
	private long startTime = 0L;
	private long targetTime = 0L;
	private long delaySeconds = DataGenerator4large.hourSeconds * 4;
	private int intervalSeconds = 3600;
	private int multiples = 4;
	private boolean isPlayback = false;

	public void run() {
		if (name.contains("gps")) {
			setRootPath(rootPath + "/gps");
			Iterator<Entry<Integer, String[]>> iterator = devGroupMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String[]> entry = iterator.next();
				int groupId = entry.getKey();
				long start = System.currentTimeMillis();
				generateGpsDataGroupNoSorted(entry.getValue(), groupId, multiples, intervalSeconds, delaySeconds,
						startTime, targetTime, isPlayback);
				long end = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + " gps time spent " + (end - start) / 1000d);
			}
			DataGenHelper4large.finishOneGpsGenerator();
		}
		if (name.contains("obd")) {
			setRootPath(rootPath + "/obd");
			Iterator<Entry<Integer, String[]>> iterator = devGroupMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String[]> entry = iterator.next();
				int groupId = entry.getKey();
				long start = System.currentTimeMillis();
				generateObdDataGroupNoSorted(entry.getValue(), groupId, multiples, intervalSeconds, delaySeconds,
						startTime, targetTime, isPlayback);
				long end = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + " obd time spent " + (end - start) / 1000d);
			}
			DataGenHelper4large.finishOneObdGenerator();
		}
	}

	public DataGenerator4large(String name) {
		this.name = name;
	}

	/**
	 * 
	 * @param devList
	 * @param groupId
	 * @param multiples
	 * @param intervalSeconds
	 * @param delaySeconds
	 *            这个参数不能取得很大，我们在这里取2s
	 * @param startTime
	 * @param targetTime
	 */
	public void generateGpsDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds + delaySeconds * multiples;
		long timeDiffSeconds = targetTime - startTime;
		HBaseScanner hs = new HBaseScanner();
		Map<Long, List<String>> msgMap = new HashMap<Long, List<String>>();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(300);
			msgMap.put(targetTime + i * 10, list);
		}
		List<String> originalList = hs.scanGPS2getMsgList(startTime, stopTime, devList);
		for (int i = 0; i < multiples; i++) {
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + "s" + i;
				long timestampTarget = jsonObject.getLong(KeyNumTimestamp) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyNumDevicesn, devicesnTarget);
				jsonObject.put(KeyNumTimestamp, timestampTarget);
				if ((timestampTarget - targetTime) >= 0) {
					long timeKey = (timestampTarget - targetTime) / 10 * 10 + targetTime;
					if (msgMap.containsKey(timeKey)) {
						List<String> messageList = msgMap.get(timeKey);
						messageList.add(jsonObject.toString());
					}
				}
			}
			if ((i != 0 && i % 80 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateGpsFileName(groupId, devList.length * multiples, entry.getKey(),
							intervalSeconds / 180), entry.getValue());
					entry.getValue().clear();
				}
			}
		}
		hs.closeHTable();
		System.gc();
	}

	public void generateGpsDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime, boolean isPlayback) {
		long stopTime = startTime + intervalSeconds + delaySeconds * multiples;
		long timeDiffSeconds = targetTime - startTime;
		HBaseScanner hs = new HBaseScanner();
		Map<Long, List<String>> msgMap = new HashMap<Long, List<String>>();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(300);
			msgMap.put(targetTime + i * 10, list);
		}
		List<String> originalList = hs.scanGPS2getMsgList(startTime, stopTime, devList);
		for (int i = 0; i < multiples; i++) {
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget;
				if (isPlayback) {
					devicesnTarget = jsonObject.getString(KeyNumDevicesn);
				} else {
					devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + "s" + i;
				}
				long timestampTarget = jsonObject.getLong(KeyNumTimestamp) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyNumDevicesn, devicesnTarget);
				jsonObject.put(KeyNumTimestamp, timestampTarget);
				if ((timestampTarget - targetTime) >= 0) {
					long timeKey = (timestampTarget - targetTime) / 10 * 10 + targetTime;
					if (msgMap.containsKey(timeKey)) {
						List<String> messageList = msgMap.get(timeKey);
						messageList.add(jsonObject.toString());
					}
				}
			}
			if ((i != 0 && i % 80 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateGpsFileName(groupId, devList.length * multiples, entry.getKey(),
							intervalSeconds / 180), entry.getValue());
					entry.getValue().clear();
				}
			}
		}
		hs.closeHTable();
		System.gc();
	}

	public void generateObdDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds + delaySeconds * multiples;
		long timeDiffSeconds = targetTime - startTime;
		Map<Long, List<String>> msgMap = new HashMap<Long, List<String>>();
		HBaseScanner hs = new HBaseScanner();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(intervalSeconds / 10 * multiples / 4);
			msgMap.put(targetTime + i * 10, list);
		}
		List<String> originalList = hs.scanRawOBD2getMsgList(startTime, stopTime, devList);
		for (int i = 0; i < multiples; i++) {
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget = jsonObject.getString(KeyDevicesn) + "_" + "s" + i;
				long timestampTarget = jsonObject.getLong(KeyGpstime) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyDevicesn, devicesnTarget);
				jsonObject.put(KeyGpstime, timestampTarget);
				if ((timestampTarget - targetTime) >= 0) {
					long timeKey = (timestampTarget - targetTime) / 10 * 10 + targetTime;
					if (msgMap.containsKey(timeKey)) {
						List<String> messageList = msgMap.get(timeKey);
						messageList.add(jsonObject.toString());
					}
				}

			}
			if ((i != 0 && i % 40 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateObdFileName(groupId, devList.length * multiples, entry.getKey(),
							intervalSeconds / 180), entry.getValue());
					entry.getValue().clear();
				}
			}
		}
		hs.closeHTable();
		System.gc();
	}

	public void generateObdDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime, boolean isPlayback) {
		long stopTime = startTime + intervalSeconds + delaySeconds * multiples;
		long timeDiffSeconds = targetTime - startTime;
		Map<Long, List<String>> msgMap = new HashMap<Long, List<String>>();
		HBaseScanner hs = new HBaseScanner();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(intervalSeconds / 10 * multiples / 4);
			msgMap.put(targetTime + i * 10, list);
		}
		List<String> originalList = hs.scanRawOBD2getMsgList(startTime, stopTime, devList);
		for (int i = 0; i < multiples; i++) {
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget;
				if (isPlayback) {
					devicesnTarget = jsonObject.getString(KeyNumDevicesn);
				} else {
					devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + "s" + i;
				}
				long timestampTarget = jsonObject.getLong(KeyGpstime) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyDevicesn, devicesnTarget);
				jsonObject.put(KeyGpstime, timestampTarget);
				if ((timestampTarget - targetTime) >= 0) {
					long timeKey = (timestampTarget - targetTime) / 10 * 10 + targetTime;
					if (msgMap.containsKey(timeKey)) {
						List<String> messageList = msgMap.get(timeKey);
						messageList.add(jsonObject.toString());
					}
				}

			}
			if ((i != 0 && i % 40 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateObdFileName(groupId, devList.length * multiples, entry.getKey(),
							intervalSeconds / 180), entry.getValue());
					entry.getValue().clear();
				}
			}
		}
		hs.closeHTable();
		System.gc();
	}

	public void tryAddList(int groupId, String[] devList) {
		devGroupMap.put(groupId, devList);
	}

	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setTargetTime(long targetTime) {
		this.targetTime = targetTime;
	}

	public void setDelaySeconds(long delaySeconds) {
		this.delaySeconds = delaySeconds;
	}

	public void setIntervalSeconds(int intervalSeconds) {
		this.intervalSeconds = intervalSeconds;
	}

	public void setMultiples(int multiples) {
		this.multiples = multiples;
	}

	private void plant(String fileName, List<String> list) {
		synchronized (lock) {
			FileWriter writer = null;
			try {
				writer = new FileWriter(new File(rootPath + "/" + fileName), true);// true表示追加
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			Iterator<String> iterator = list.iterator();
			while (iterator.hasNext()) {
				try {
					writer.write(iterator.next() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 根据数据组生成器属性产生文件名
	 * 
	 * @param groupId
	 *            指数据组id
	 * @param devListSize
	 *            指数据组车辆数
	 * @param timeId
	 *            指该文件数据的开始时间
	 * @param intervalSeconds
	 *            指数据包含的时间范围
	 * @param rep
	 * @return
	 */
	private String generateGpsFileName(int groupId, int groupSize, long timeId, int intervalSeconds) {
		// return timeId + "_" + intervalSeconds + "_" + groupId + "_" +
		// groupSize + "_gps.txt";
		return timeId + "_" + intervalSeconds + "_" + timeId + "_" + groupSize + "_gps.txt";

	}

	private String generateObdFileName(int groupId, int groupSize, long timeId, int intervalSeconds) {
		// return timeId + "_" + intervalSeconds + "_" + groupId + "_" +
		// groupSize + "_obd.txt";
		return timeId + "_" + intervalSeconds + "_" + timeId + "_" + groupSize + "_obd.txt";
	}

	public void setIsPlayback(boolean isPlayback) {
		this.isPlayback = isPlayback;
	}
}