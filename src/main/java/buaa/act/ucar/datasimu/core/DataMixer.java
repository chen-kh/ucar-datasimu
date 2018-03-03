package buaa.act.ucar.datasimu.core;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;

import buaa.act.ucar.datasimu.Simulator;
import buaa.act.ucar.datasimu.core2.DataMixHelper2;

public class DataMixer extends Thread {
	private Map<Integer, File[]> groupsMap = new HashMap<Integer, File[]>();
	private Map<Long, LinkedBlockingQueue<String>> msgQueueMap;
	private BufferedReader bufferedReader;
	private String rootPath = getRealPath() + "/mixed/";
	private String name;
	private int intervalSeconds;
	private DataMixer.Splitor[] splitors = null;
	private DataMixer.Planter[] planters = null;
	private static AtomicInteger splitorFinishNum = new AtomicInteger(0);
	private static AtomicInteger planterFinishNum = new AtomicInteger(0);

	public DataMixer(String name, String rootPath, int intervalSeconds) {
		this.name = name;
		this.rootPath = rootPath;
		this.intervalSeconds = intervalSeconds;
		msgQueueMap = new HashMap<Long, LinkedBlockingQueue<String>>(10);
	}

	public void tryAddFileGroup(int groupId, File[] files) {
		groupsMap.put(groupId, files);
	}

	public void run() {
		if (name.contains("gps")) {
			rootPath += "/gps";
			Iterator<Entry<Integer, File[]>> iterator = groupsMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, File[]> entry = iterator.next();
				int groupId = entry.getKey();
				File[] files = entry.getValue();
				// ArrayList<SortedMap<Long, List<String>>> list =
				// splitFilesByTimestamp(files, intervalSeconds);
				String[] infos = files[0].getName().split("_");// groupId_groupLength_timeId_intervalHours_ri.txt
				long initTimeId = Long.parseLong(infos[0]);
				int groupSize = Integer.parseInt(infos[3]) * files.length;
				long intervalSeconds4EachOriginalFile = Long.parseLong(infos[1]);
				// generateMixedFiles(groupId, groupSize, initTimeId, list);
				splitorFinishNum.set(0);
				planterFinishNum.set(0);
				// 初始化map
				msgQueueMap.clear();
				System.out.println("msgQueueMap is null now");
				for (long timeId = initTimeId; timeId < initTimeId
						+ intervalSeconds4EachOriginalFile; timeId += intervalSeconds) {
					LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(200 * 1000);
					msgQueueMap.put(timeId, queue);
				}
				long start = System.currentTimeMillis();
				splitFilesByTimestampUsingSplitor(files, intervalSeconds, 20);
				generateMixedFilesUsingPlanter(groupId, groupSize, initTimeId, msgQueueMap);
				waitUtilFinishMixPresentFiles(msgQueueMap.size());
				long end = System.currentTimeMillis();
				System.out.println(
						"splitFilesByTimestampUsingSplitor time spent = " + (double) (end - start) / 1000 + "s");
			}
			groupsMap.clear();
			reportFinishOneGpsMixTask();
		}

		// if (name.contains("obd")) {
		// rootPath += "/obd";
		// Iterator<Entry<Integer, File[]>> iterator =
		// groupsMap.entrySet().iterator();
		// while (iterator.hasNext()) {
		// Entry<Integer, File[]> entry = iterator.next();
		// int groupId = entry.getKey();
		// File[] files = entry.getValue();
		// long start = System.currentTimeMillis();
		// ArrayList<SortedMap<Long, List<String>>> list =
		// splitFilesByTimestamp(files, intervalSeconds);
		// long end = System.currentTimeMillis();
		// System.out.println("splitFilesByTimestamp time spent = " + (double)
		// (end - start) / 1000 + "s");
		// String[] infos = files[0].getName().split("_");//
		// groupId_groupLength_timeId_intervalHours_ri.txt
		// long initTimeId = Long.parseLong(infos[2]);
		// int groupSize = Integer.parseInt(infos[1]) * files.length;
		// generateMixedFiles(groupId, groupSize, initTimeId, list);
		// }
		// }
		if (name.contains("obd")) {
			rootPath += "/obd";
			Iterator<Entry<Integer, File[]>> iterator = groupsMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Integer, File[]> entry = iterator.next();
				int groupId = entry.getKey();
				File[] files = entry.getValue();
				String[] infos = files[0].getName().split("_");// timeId_intervalHours_groupId_groupLength_ri.txt
				long initTimeId = Long.parseLong(infos[0]);
				int groupSize = Integer.parseInt(infos[3]) * files.length;
				long intervalSeconds4EachOriginalFile = Long.parseLong(infos[1]);
				splitorFinishNum.set(0);
				planterFinishNum.set(0);
				// 初始化map
				msgQueueMap.clear();
				System.out.println("msgQueueMap is null now");
				for (long timeId = initTimeId; timeId < initTimeId
						+ intervalSeconds4EachOriginalFile; timeId += intervalSeconds) {
					LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(200 * 1000);
					msgQueueMap.put(timeId, queue);
				}
				long start = System.currentTimeMillis();
				splitFilesByTimestampUsingSplitor(files, intervalSeconds, 20);
				generateMixedFilesUsingPlanter(groupId, groupSize, initTimeId, msgQueueMap);
				waitUtilFinishMixPresentFiles(msgQueueMap.size());
				long end = System.currentTimeMillis();
				System.out.println(
						"splitFilesByTimestampUsingSplitor time spent = " + (double) (end - start) / 1000 + "s");
			}
			groupsMap.clear();
			reportFinishOneObdMixTask();
		}
	}

	private void reportFinishOneObdMixTask() {
		DataMixHelper2.obdMixerFinishNum.incrementAndGet();
	}

	private void reportFinishOneGpsMixTask() {
		DataMixHelper2.gpsMixerFinishNum.incrementAndGet();
	}

	private void waitUtilFinishMixPresentFiles(int planterNum) {
		while (planterFinishNum.get() != planterNum) {
			try {
				System.out.println("DataMixer sleeping ...");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void generateMixedFiles(int groupId, int groupSize, long initTimeId,
			ArrayList<SortedMap<Long, List<String>>> list) {
		for (int i = 0; i < list.size(); i++) {
			long timeId = initTimeId + i * 100;
			String fileName = generateFileName(groupId, groupSize, timeId, intervalSeconds);
			plant(fileName, list.get(i));
			list.get(i).clear();
		}
	}

	private void generateMixedFilesUsingPlanter(int groupId, int groupSize, long initTimeId,
			Map<Long, LinkedBlockingQueue<String>> map) {
		Iterator<Long> keyIt = map.keySet().iterator();
		// 初始化planters
		planters = new Planter[map.size()];
		int id_planter = 0;
		while (keyIt.hasNext()) {
			long timeId = keyIt.next();
			String fileName = generateFileName(groupId, groupSize, timeId, intervalSeconds);
			DataMixer.Planter planter = new Planter(timeId, fileName, map.get(timeId));
			planter.start();
			planters[id_planter++] = planter;
		}
		if (id_planter != map.size()) {
			System.err.println("the number of planter !!!!===== map.size");
			System.exit(0);
		}
	}

	private void plant(String fileName, SortedMap<Long, List<String>> sortedMap) {
		File file = new File(rootPath + "/" + fileName);
		BufferedWriter bufferedWriter = null;
		try {
			FileWriter fileWriter = new FileWriter(file);
			bufferedWriter = new BufferedWriter(fileWriter);
			Iterator<Entry<Long, List<String>>> iterator = sortedMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Long, List<String>> entry = iterator.next();
				for (String msg : entry.getValue()) {
					bufferedWriter.write(msg + "\n");
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

	private void addMessage(Long timestamp, final String gpsMessage, SortedMap<Long, List<String>> map) {
		List<String> messageList = map.getOrDefault(timestamp, new ArrayList<String>());
		messageList.add(gpsMessage);
		map.put(timestamp, messageList);
	}

	private ArrayList<SortedMap<Long, List<String>>> splitFilesByTimestamp(File[] files,
			int intervalSeconds4targetFile) {
		String[] infos = files[0].getName().split("_");// groupId_groupLength_timeId_intervalHours_ri.txt
		long initTimeId = Long.parseLong(infos[2]);
		int intervalSeconds = Integer.parseInt(infos[3]);
		ArrayList<SortedMap<Long, List<String>>> list = new ArrayList<SortedMap<Long, List<String>>>();
		for (int i = 0; i < intervalSeconds / intervalSeconds4targetFile; i++) {
			SortedMap<Long, List<String>> map = new TreeMap<Long, List<String>>(new Comparator<Long>() {
				public int compare(Long arg0, Long arg1) {
					return (int) (arg0 - arg1);
				}
			});
			list.add(map);
		}
		for (File file : files) {
			try {
				bufferedReader = new BufferedReader(new FileReader(file));
				String temp = null;
				while ((temp = bufferedReader.readLine()) != null) {
					JSONObject jo = new JSONObject(temp);
					long timestamp = jo.getLong("gpstime");
					// long startTime = initTimeId;
					int timeId = (int) ((timestamp - initTimeId) / intervalSeconds4targetFile);
					addMessage(timestamp, jo.toString(), list.get(timeId));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 重载方法，使用多线程来切分文件（主要原因是文件数量比较大，一个一个读取很慢）
	 * 
	 * @param files
	 * @param intervalSeconds4targetFile
	 * @param threadNum
	 * @return
	 */
	private void splitFilesByTimestampUsingSplitor(File[] files, int intervalSeconds4targetFile, int threadNum) {
		String[] infos = files[0].getName().split("_");// groupId_groupLength_timeId_intervalHours_ri.txt
		long initTimeId = Long.parseLong(infos[0]);
		// int intervalSeconds = Integer.parseInt(infos[3]);
		// 初始化splitors,并启动所有的splitors
		splitors = new Splitor[threadNum];
		for (int i = 0; i < threadNum; i++) {
			Splitor splitor = new Splitor(initTimeId, intervalSeconds4targetFile);
			splitors[i] = splitor;
		}
		// 分配并添加文件到splitor中
		for (int i = 0; i < files.length; i++) {
			splitors[i % threadNum].tryAddFile(files[i]);
		}
		for (int i = 0; i < threadNum; i++) {
			splitors[i].start();
		}
	}

	private String getRealPath() {
		String rootPath = Simulator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}

	class Splitor extends Thread {
		// 使用queue的原因是不希望一直在内存里面加载大量的文件内容
		private List<File> fileList = new ArrayList<File>();
		private BufferedReader bufferedReader = null;
		long initTimeId;
		int intervalSeconds4targetFile;
		private String KeyTimestamp;

		public Splitor(long initTimeId, int intervalSeconds4targetFile) {
			this.initTimeId = initTimeId;
			this.intervalSeconds4targetFile = intervalSeconds4targetFile;
			if (name.contains("gps"))
				KeyTimestamp = "0";
			if (name.contains("obd"))
				KeyTimestamp = "gpstime";
		}

		public void tryAddFile(File file) {
			fileList.add(file);
		}

		public void closeBufferedReader() {
			try {
				bufferedReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void splitFile(File file, long initTimeId, int intervalSeconds4targetFile) {
			long timeId = 0L;
			try {
				bufferedReader = new BufferedReader(new FileReader(file));
				String temp = null;
				while ((temp = bufferedReader.readLine()) != null) {
					JSONObject jo = new JSONObject(temp);
					long timestamp = jo.getLong(KeyTimestamp);
					timeId = (timestamp - initTimeId) / intervalSeconds4targetFile * intervalSeconds4targetFile
							+ initTimeId;
					msgQueueMap.get(timeId).put(jo.toString());// 队列满之后会阻塞
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			closeBufferedReader();
		}

		public void run() {
			for (File file : fileList) {
				splitFile(file, initTimeId, intervalSeconds4targetFile);
			}
			reportFinish();
		}

		private void reportFinish() {
			splitorFinishNum.incrementAndGet();
		}
	}

	class Planter extends Thread {
		private long timeId;
		private String fileName;
		private LinkedBlockingQueue<String> queue;
		private String KeyTimestamp;

		public Planter(long timeId, String fileName, LinkedBlockingQueue<String> queue) {
			this.timeId = timeId;
			this.fileName = fileName;
			this.queue = queue;
			if (name.contains("gps"))
				KeyTimestamp = "0";
			if (name.contains("obd"))
				KeyTimestamp = "gpstime";
		}

		public void run() {
			SortedMap<Long, List<String>> map = new TreeMap<Long, List<String>>(new Comparator<Long>() {
				public int compare(Long arg0, Long arg1) {
					return (int) (arg0 - arg1);
				}
			});
			while (!finishThisGroup()) {
				try {
					if (queue.isEmpty()) {
						sleep(80);
						continue;
					}
					JSONObject jo = new JSONObject(queue.take());
					long timestamp = jo.getLong(KeyTimestamp);
					addMessage(timestamp, jo.toString(), map);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			while (!queue.isEmpty()) {
				try {
					JSONObject jo = new JSONObject(queue.take());
					long timestamp = jo.getLong(KeyTimestamp);
					addMessage(timestamp, jo.toString(), map);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			plant(fileName, map);
			reportFinish();
			map.clear();
		}

		private void reportFinish() {
			planterFinishNum.incrementAndGet();
		}

		private boolean finishThisGroup() {
			if (splitorFinishNum.get() == 20) {
				return true;
			}
			return false;
		}
	}
}
