package buaa.act.ucar.datasimu.core2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
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

import net.sf.json.JSONObject;

public class DataMixer2 extends Thread {
	private Map<Integer, File[]> groupsMap = new HashMap<Integer, File[]>();
	private SortedMap<Long, List<String>> sortedMsgMap;
	private String rootPath;
	private String name;
	private int intervalSeconds;

	public DataMixer2(String name, int intervalSeconds) {
		this.name = name;
		this.intervalSeconds = intervalSeconds;
		this.sortedMsgMap = new TreeMap<Long, List<String>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
	}
	public void setRootPath(String rootPath){
		this.rootPath = rootPath;
	}
	public void tryAddFileGroup(int groupId, File[] files) {
		groupsMap.put(groupId, files);
	}

	public void run() {
		if (name.contains("gps")) {
			rootPath += "/gps";
			Iterator<Entry<Integer, File[]>> iterator = groupsMap.entrySet().iterator();
			final String KeyNumTimestamp = "0";
			while (iterator.hasNext()) {
				Entry<Integer, File[]> entry = iterator.next();
				int groupId = entry.getKey();
				File[] files = entry.getValue();
				String[] infos = files[0].getName().split("_");// groupId_groupLength_timeId_intervalHours_ri.txt
				int groupSize = Integer.parseInt(infos[3]) * files.length;
				for (File file : files) {
					BufferedReader reader = null;
					try {
						reader = new BufferedReader(new FileReader(file));
						String temp = null;
						while ((temp = reader.readLine()) != null) {
							JSONObject jo = JSONObject.fromObject(temp);
							long timeId = Long.parseLong(jo.getString(KeyNumTimestamp));
							List<String> list = sortedMsgMap.getOrDefault(timeId, new ArrayList<String>());
							list.add(temp);
							sortedMsgMap.put(timeId, list);
						}
						reader.close();
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
				plant(generateFileName(groupId, groupSize, groupId, intervalSeconds), sortedMsgMap);
				sortedMsgMap.clear();
			}
			groupsMap.clear();
			groupsMap = null;
			reportFinishOneGpsMixTask();
		}
		if (name.contains("obd")) {
			rootPath += "/obd";
			Iterator<Entry<Integer, File[]>> iterator = groupsMap.entrySet().iterator();
			final String KeyGpstime = "gpstime";
			while (iterator.hasNext()) {
				Entry<Integer, File[]> entry = iterator.next();
				int groupId = entry.getKey();
				File[] files = entry.getValue();
				String[] infos = files[0].getName().split("_");// groupId_groupLength_timeId_intervalHours_ri.txt
				int groupSize = Integer.parseInt(infos[3]) * files.length;
				for (File file : files) {
					BufferedReader reader = null;
					try {
						reader = new BufferedReader(new FileReader(file));
						String temp = null;
						while ((temp = reader.readLine()) != null) {
//							JSONObject jo = null;
//							try {
//								jo = JSONObject.fromObject(temp);
//							} catch (Exception e) {
//								continue;
//							}
							JSONObject jo = JSONObject.fromObject(temp);
							long timeId = Long.parseLong(jo.getString(KeyGpstime));
							List<String> list = sortedMsgMap.getOrDefault(timeId, new ArrayList<String>());
							list.add(temp);
							sortedMsgMap.put(timeId, list);
						}
						reader.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				plant(generateFileName(groupId, groupSize, groupId, 10), sortedMsgMap);
				sortedMsgMap.clear();
			}
			groupsMap.clear();
			groupsMap = null;
			reportFinishOneObdMixTask();
		}
	}

	private void reportFinishOneObdMixTask() {
		DataMixHelper2.obdMixerFinishNum.incrementAndGet();
	}

	private void reportFinishOneGpsMixTask() {
		DataMixHelper2.gpsMixerFinishNum.incrementAndGet();
	}

	private void plant(String fileName, SortedMap<Long, List<String>> sortedMap) {
		File file = new File(rootPath + "/" + fileName);
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(file));
			Iterator<Entry<Long, List<String>>> iterator = sortedMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Long, List<String>> entry = iterator.next();
				List<String> list = entry.getValue();
				Iterator<String> it = list.iterator();
				while (it.hasNext()) {
					bufferedWriter.write(it.next() + "\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("plant messages to " + fileName);
	}

	private String generateFileName(int groupId, int groupSize, long timeId, int intervalSeconds) {
		if (name.contains("gps"))
			return timeId + "_" + intervalSeconds + "_" + groupId + "_" + groupSize + "_gps.txt";
		if (name.contains("obd"))
			return timeId + "_" + intervalSeconds + "_" + groupId + "_" + groupSize + "_obd.txt";
		return null;
	}
}
