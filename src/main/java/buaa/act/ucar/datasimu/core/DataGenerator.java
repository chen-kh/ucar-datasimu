package buaa.act.ucar.datasimu.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONObject;

import buaa.act.ucar.datasimu.Simulator;
import buaa.act.ucar.datasimu.hbase.HBaseScanner;

public class DataGenerator extends Thread {
	private String name;
	private Map<Integer, String[]> map = new HashMap<Integer, String[]>();// 作为这个生成器持有的groupId->devList对儿
	private static final String KeyNumTimestamp = "0";
	private static final String KeyNumDevicesn = "1";
	private static final String KeyDevicesn = "devicesn";
	private static final String KeyGpstime = "gpstime";
	public static final long hourSeconds = 60 * 60L;
	public static final long daySeconds = 24 * hourSeconds;
	private FileWriter fileWriter;
	private String rootPath = getRealPath() + "/created";// 写入的根目录
	// private String dateTarget = "2017-01-18 00:00:00";
	// private String dateOriginal = "2016-10-12 00:00:00";
	private long startTime = 0L;
	private long targetTime = 0L;
	private long delaySeconds = DataGenerator.hourSeconds * 4;
	private int intervalSeconds = 3600;
	private int multiples = 4;

	public void run() {
		if (name.contains("gps")) {
			setRootPath(getRootPath() + "/gps");
			Iterator<Entry<Integer, String[]>> iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String[]> entry = iterator.next();
				int groupId = entry.getKey();
				// generateGpsDataGroup(entry.getValue(), groupId, multiples,
				// intervalSeconds, delaySeconds,
				// startTime, targetTime);
				generateGpsDataGroupNoSorted(entry.getValue(), groupId, multiples, intervalSeconds, delaySeconds,
						startTime, targetTime);

			}
			DataGenHelper.finishOneGpsGenerator();
		}
		if (name.contains("obd")) {
			setRootPath(getRootPath() + "/obd");
			Iterator<Entry<Integer, String[]>> iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String[]> entry = iterator.next();
				int groupId = entry.getKey();
				generateObdDataGroupNoSorted(entry.getValue(), groupId, multiples, intervalSeconds, delaySeconds,
						startTime, targetTime);
				// generateObdDataGroup(entry.getValue(), groupId, multiples,
				// intervalSeconds, delaySeconds, startTime,
				// targetTime);
			}
			DataGenHelper.finishOneObdGenerator();
		}
	}

	public DataGenerator(String name) {
		this.name = name;
	}

	/**
	 * 
	 * @param devList
	 * @param multiples
	 * @param intervalSeconds
	 * @param delaySeconds
	 * @param startTime
	 *            重载方法，时间单位为S
	 * @param targetTime
	 */
	public void generateGpsDataGroup(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds - 1;
		long timeDiffSeconds = targetTime - startTime;
		long timeId = targetTime;
		HBaseScanner hs = new HBaseScanner();
		SortedMap<Long, List<String>> targetMap = null;
		for (int i = 0; i < multiples; i++) {
			SortedMap<Long, List<String>> originalMap = hs.scanGPS(startTime + delaySeconds * i,
					stopTime + delaySeconds * i, devList);
			targetMap = replaceDevicesnAndTimestamp_gps(originalMap, "s" + i, timeDiffSeconds - delaySeconds * i);
			plant(generateGpsFileName(groupId, devList.length, timeId, intervalSeconds, i), targetMap);
		}
		hs.closeHTable();
	}

	public void generateGpsDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		long timeId = targetTime;
		HBaseScanner hs = new HBaseScanner();
		List<String> targetList = null;
		for (int i = 0; i < multiples; i++) {
			List<String> originalList = hs.scanGPS2getMsgList(startTime + delaySeconds * i, stopTime + delaySeconds * i,
					devList);
			targetList = replaceDevicesnAndTimestamp_gps(originalList, "s" + i, timeDiffSeconds - delaySeconds * i);
			plant(generateGpsFileName(groupId, devList.length, timeId, intervalSeconds, i), targetList);
		}
		hs.closeHTable();
	}

	public void generateObdDataGroup(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		long timeId = targetTime;
		HBaseScanner hs = new HBaseScanner();
		SortedMap<Long, List<String>> targetMap = null;
		for (int i = 0; i < multiples; i++) {
			SortedMap<Long, List<String>> originalMap = hs.scanRawOBD(startTime + delaySeconds * i,
					stopTime + delaySeconds * i, devList);
			targetMap = replaceDevicesnAndTimestamp_obd(originalMap, "s" + i, timeDiffSeconds - delaySeconds * i);
			plant(generateObdFileName(groupId, devList.length, timeId, intervalSeconds, i), targetMap);
		}
		hs.closeHTable();
	}

	public void generateObdDataGroupNoSorted(String[] devList, int groupId, int multiples, int intervalSeconds,
			long delaySeconds, long startTime, long targetTime) {
		long stopTime = startTime + intervalSeconds;
		long timeDiffSeconds = targetTime - startTime;
		long timeId = targetTime;
		HBaseScanner hs = new HBaseScanner();
		List<String> targetList = null;
		for (int i = 0; i < multiples; i++) {
			List<String> originalList = hs.scanRawOBD2getMsgList(startTime + delaySeconds * i,
					stopTime + delaySeconds * i, devList);
			targetList = replaceDevicesnAndTimestamp_obd(originalList, "s" + i, timeDiffSeconds - delaySeconds * i);
			plant(generateObdFileName(groupId, devList.length, timeId, intervalSeconds, i), targetList);
		}
		hs.closeHTable();
	}

	// public void setDateTarget(String dateTarget) {
	// this.dateTarget = dateTarget;
	// }
	//
	// public void setDateOriginal(String dateOriginal) {
	// this.dateOriginal = dateOriginal;
	// }

	public String getRootPath() {
		return rootPath;
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

	public void tryAddList(int groupId, String[] devList) {
		map.put(groupId, devList);
	}

	private String getRealPath() {
		String rootPath = Simulator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}

	private void plant(String fileName, SortedMap<Long, List<String>> msgMapKeyedByTimestamp) {
		try {
			fileWriter = new FileWriter(new File(getRootPath() + "/" + fileName));// true表示追加
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		Iterator<Entry<Long, List<String>>> iterator = msgMapKeyedByTimestamp.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<Long, List<String>> entry = iterator.next();
			plantMessages(fileName, entry.getValue());
		}
		try {
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void plant(String fileName, List<String> list) {
		try {
			fileWriter = new FileWriter(new File(getRootPath() + "/" + fileName));// true表示追加
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		Iterator<String> iterator = list.iterator();
		while (iterator.hasNext()) {
			try {
				fileWriter.write(iterator.next() + "\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void plantMessages(String fileName, List<String> list) {
		for (String msg : list) {
			try {
				fileWriter.write(msg + "\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private SortedMap<Long, List<String>> replaceDevicesnAndTimestamp_gps(SortedMap<Long, List<String>> originalMap,
			String suffix, long timeDiffSeconds) {
		SortedMap<Long, List<String>> targetMap = new TreeMap<Long, List<String>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
		Iterator<Entry<Long, List<String>>> iterator = originalMap.entrySet().iterator();
		List<String> listOriginal = new ArrayList<String>();
		long timestampTarget = 0L;
		while (iterator.hasNext()) {
			List<String> listTarget = new ArrayList<String>();
			Entry<Long, List<String>> temp = iterator.next();
			timestampTarget = temp.getKey() + timeDiffSeconds;
			listOriginal = temp.getValue();
			JSONObject jsonObject = null;
			for (String msg : listOriginal) {
				jsonObject = new JSONObject(msg);
				String devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + suffix;
				jsonObject.put(KeyNumDevicesn, devicesnTarget);
				jsonObject.put(KeyNumTimestamp, timestampTarget);
				listTarget.add(jsonObject.toString());
			}
			targetMap.put(timestampTarget, listTarget);
		}
		return targetMap;
	}

	private List<String> replaceDevicesnAndTimestamp_gps(List<String> originalList, String suffix,
			long timeDiffSeconds) {
		List<String> listTarget = new ArrayList<String>();
		Iterator<String> iterator = originalList.iterator();
		while (iterator.hasNext()) {
			JSONObject jsonObject = null;
			jsonObject = new JSONObject(iterator.next());
			String devicesnTarget = jsonObject.getString(KeyNumDevicesn) + "_" + suffix;
			long timestampTarget = jsonObject.getLong(KeyNumTimestamp) + timeDiffSeconds;
			jsonObject.put(KeyNumDevicesn, devicesnTarget);
			jsonObject.put(KeyNumTimestamp, timestampTarget);
			listTarget.add(jsonObject.toString());
		}
		originalList.clear();
		return listTarget;
	}

	private SortedMap<Long, List<String>> replaceDevicesnAndTimestamp_obd(SortedMap<Long, List<String>> originalMap,
			String suffix, long timeDiffSeconds) {
		SortedMap<Long, List<String>> targetMap = new TreeMap<Long, List<String>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
		Iterator<Entry<Long, List<String>>> iterator = originalMap.entrySet().iterator();
		List<String> listOriginal = new ArrayList<String>();
		long timestampTarget = 0L;
		while (iterator.hasNext()) {
			List<String> listTarget = new ArrayList<String>();
			Entry<Long, List<String>> temp = iterator.next();
			timestampTarget = temp.getKey() + timeDiffSeconds;
			listOriginal = temp.getValue();
			JSONObject jsonObject = null;
			for (String msg : listOriginal) {
				jsonObject = new JSONObject(msg);
				String devicesnTarget = jsonObject.getString(KeyDevicesn) + "_" + suffix;
				jsonObject.put(KeyDevicesn, devicesnTarget);
				jsonObject.put(KeyGpstime, timestampTarget);
				listTarget.add(jsonObject.toString());
			}
			listOriginal.clear();
			targetMap.put(timestampTarget, listTarget);
		}
		return targetMap;
	}

	private List<String> replaceDevicesnAndTimestamp_obd(List<String> originalList, String suffix,
			long timeDiffSeconds) {
		List<String> listTarget = new ArrayList<String>();
		Iterator<String> iterator = originalList.iterator();
		while (iterator.hasNext()) {
			JSONObject jsonObject = null;
			jsonObject = new JSONObject(iterator.next());
			String devicesnTarget = jsonObject.getString(KeyDevicesn) + "_" + suffix;
			long timestampTarget = jsonObject.getLong(KeyGpstime) + timeDiffSeconds;
			jsonObject.put(KeyDevicesn, devicesnTarget);
			jsonObject.put(KeyGpstime, timestampTarget);
			listTarget.add(jsonObject.toString());
		}
		originalList.clear();
		return listTarget;
	}

	/**
	 * 根据数据组生成器属性产生文件名
	 * 
	 * @param groupId
	 *            指数据组id
	 * @param devListSize
	 *            指数据组车辆数
	 * @param timeId
	 *            指从2014-01-01 00:00:00以来到制定时间的小时数
	 * @param intervalSeconds
	 *            指数据包含的时间范围
	 * @param rep
	 * @return
	 */
	private String generateGpsFileName(int groupId, int devListSize, long timeId, int intervalSeconds, int repId) {
		return timeId + "_" + intervalSeconds + "_" + groupId + "_" + devListSize + "_r" + repId + "_gps.txt";
	}

	private String generateObdFileName(int groupId, int devListSize, long timeId, int intervalSeconds, int repId) {
		return timeId + "_" + intervalSeconds + "_" + groupId + "_" + devListSize + "_r" + repId + "_obd.txt";
	}
}