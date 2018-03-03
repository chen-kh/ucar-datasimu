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

import org.apache.commons.io.output.ThresholdingOutputStream;
import org.json.JSONObject;

import buaa.act.ucar.datasimu.hbase.HBaseScanner;

public class DataGenerator2 extends Thread {
	private static Object gpsLlock = new Object();
	private static Object obdLock = new Object();
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
	private long delaySeconds = DataGenerator2.hourSeconds * 4;
	private int intervalSeconds = 3600;
	private int multiples = 4;
	private Map<Long, List<String>> msgMap = new HashMap<Long, List<String>>();
	private boolean isPlayback = false;
	public void run() {
		if (name.contains("gps")) {
			setRootPath(rootPath + "/gps");
			Iterator<Entry<Integer, String[]>> iterator = devGroupMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String[]> entry = iterator.next();
				int groupId = entry.getKey();
//				long start = System.currentTimeMillis();
				generateGpsDataGroupNoSorted(entry.getValue(), groupId, multiples, intervalSeconds, delaySeconds,
						startTime, targetTime, isPlayback);
//				long end = System.currentTimeMillis();
//				System.out.println(Thread.currentThread().getName() + " gps time spent " + (end - start) / 1000d);
			}
			DataGenHelper2.finishOneGpsGenerator();
		}
		if (name.contains("obd")) {
			setRootPath(rootPath + "/obd");
			Iterator<Entry<Integer, String[]>> iterator = devGroupMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String[]> entry = iterator.next();
				int groupId = entry.getKey();
//				long start = System.currentTimeMillis();
				generateObdDataGroupNoSorted(entry.getValue(), groupId, multiples, intervalSeconds, delaySeconds,
						startTime, targetTime, isPlayback);
//				long end = System.currentTimeMillis();
//				System.out.println(Thread.currentThread().getName() + " obd time spent " + (end - start) / 1000d);
			}
			DataGenHelper2.finishOneObdGenerator();
		}
	}

	public DataGenerator2(String name) {
		this.name = name;
	}

	public void generateGpsDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		HBaseScanner hs = new HBaseScanner();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(300);
			msgMap.put(targetTime + i * 10, list);
		}
		for (int i = 0; i < multiples; i++) {
			List<String> originalList = hs.scanGPS2getMsgList(startTime + delaySeconds * i, stopTime + delaySeconds * i,
					devList);
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + "s" + i;
				long timestampTarget = jsonObject.getLong(KeyNumTimestamp) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyNumDevicesn, devicesnTarget);
				jsonObject.put(KeyNumTimestamp, timestampTarget);
				List<String> messageList = msgMap.get((timestampTarget - targetTime) / 10 * 10 + targetTime);
				messageList.add(jsonObject.toString());
			}
			originalList = null;
			if ((i != 0 && i % 80 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateGpsFileName(groupId, devList.length * multiples * 50, entry.getKey(),
							intervalSeconds / 180), entry.getValue());
					entry.getValue().clear();
				}
			}
		}
		hs.closeHTable();
		System.gc();
	}
	// 仅供playback的时候使用
	public void generateGpsDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime, boolean isPlayback) {
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		HBaseScanner hs = new HBaseScanner();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(300);
			msgMap.put(targetTime + i * 10, list);
		}
		for (int i = 0; i < multiples; i++) {
			List<String> originalList = hs.scanGPS2getMsgList(startTime + delaySeconds * i, stopTime + delaySeconds * i,
					devList);
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget;
				if(isPlayback){
					devicesnTarget = jsonObject.getString(KeyNumDevicesn);
				}else{
					devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + "s" + i;
				}
				long timestampTarget = jsonObject.getLong(KeyNumTimestamp) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyNumDevicesn, devicesnTarget);
				jsonObject.put(KeyNumTimestamp, timestampTarget);
				List<String> messageList = msgMap.get((timestampTarget - targetTime) / 10 * 10 + targetTime);
				messageList.add(jsonObject.toString());
			}
			originalList = null;
			if ((i != 0 && i % 80 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateGpsFileName(groupId, devList.length * multiples * 50, entry.getKey(),
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
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		HBaseScanner hs = new HBaseScanner();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(600);
			msgMap.put(targetTime + i * 10, list);
		}
		for (int i = 0; i < multiples; i++) {
			List<String> originalList = hs.scanRawOBD2getMsgList(startTime + delaySeconds * i,
					stopTime + delaySeconds * i, devList);
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget = jsonObject.getString(KeyDevicesn) + "_" + "s" + i;
				long timestampTarget = jsonObject.getLong(KeyGpstime) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyDevicesn, devicesnTarget);
				jsonObject.put(KeyGpstime, timestampTarget);
				List<String> messageList = msgMap.get((timestampTarget - targetTime) / 10 * 10 + targetTime);
				messageList.add(jsonObject.toString());
			}
			originalList = null;// originalList实在for域内声明的，
			if ((i != 0 && i % 40 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateObdFileName(groupId, devList.length * multiples * 50, entry.getKey(),
							intervalSeconds / 180), entry.getValue());
					entry.getValue().clear();
				}
			}
		}
		hs.closeHTable();
		System.gc();
	}
	// 仅供playback的时候使用
	public void generateObdDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime, boolean isPlayback) {
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		HBaseScanner hs = new HBaseScanner();
		for (int i = 0; i < 180; i++) {
			List<String> list = new ArrayList<String>(600);
			msgMap.put(targetTime + i * 10, list);
		}
		for (int i = 0; i < multiples; i++) {
			List<String> originalList = hs.scanRawOBD2getMsgList(startTime + delaySeconds * i,
					stopTime + delaySeconds * i, devList);
			Iterator<String> iterator = originalList.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = new JSONObject(iterator.next());
				String devicesnTarget;
				if(isPlayback){
					devicesnTarget = jsonObject.getString(KeyDevicesn);
				}else{
					devicesnTarget = jsonObject.getString(KeyDevicesn) + "_" + "s" + i;
				}
				long timestampTarget = jsonObject.getLong(KeyGpstime) + (timeDiffSeconds - delaySeconds * i);
				jsonObject.put(KeyDevicesn, devicesnTarget);
				jsonObject.put(KeyGpstime, timestampTarget);
				List<String> messageList = msgMap.get((timestampTarget - targetTime) / 10 * 10 + targetTime);
				messageList.add(jsonObject.toString());
			}
			originalList = null;// originalList实在for域内声明的，
			if ((i != 0 && i % 40 == 0) || i == (multiples - 1)) {
				Iterator<Entry<Long, List<String>>> it = msgMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Long, List<String>> entry = it.next();
					plant(generateObdFileName(groupId, devList.length * multiples * 50, entry.getKey(),
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
		if (name.contains("gps")) {
			synchronized (gpsLlock) {
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
		if (name.contains("obd")) {
			synchronized (obdLock) {
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

	public void setIsPlayback(boolean isPlayback2) {
		this.isPlayback = isPlayback2;
	}
}