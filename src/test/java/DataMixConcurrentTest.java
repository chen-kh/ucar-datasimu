
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONObject;

public class DataMixConcurrentTest {
	private static DataMixConcurrentTest dataMixConcurrentTest = new DataMixConcurrentTest();

	public static void main(String[] args) {
		File mixedFile = new File(
				"E://carsimu/datasimu/test_1000w/created/obd_80000/150-151_80000_1484668800_1800_obd.txt");
		File file = new File("E://carsimu/datasimu/test_1000w/created/obd");
		BufferedReader bufferedReader = null;
		String[] files = file.list();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 800; i++) {
			File file2 = new File(file.getAbsolutePath() + "/" + files[i]);
			try {
				bufferedReader = new BufferedReader(new FileReader(file2));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
			String temp = null;
			try {
				while ((temp = bufferedReader.readLine()) != null) {
					JSONObject jo = new JSONObject(temp);
					long timestamp = jo.getLong("gpstime");
					int timeId = (int) ((timestamp - 1484668800L) / 100);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("[read 800 file] time spent = " + (double) (end - start) / 1000 + "s");
		BufferedWriter bufferedWriter = null;
		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(mixedFile);
			bufferedWriter = new BufferedWriter(fileWriter);
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		long start_mix = System.currentTimeMillis();
		for (int i = 0; i < 800; i++) {
			File file2 = new File(file.getAbsolutePath() + "/" + files[i]);
			try {
				bufferedReader = new BufferedReader(new FileReader(file2));
				String temp = null;
				while ((temp = bufferedReader.readLine()) != null) {
					bufferedWriter.write(temp + "\n");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end_mix = System.currentTimeMillis();
		System.out.println("[mix 800 files] time spent = " + (double) (end_mix - start_mix) / 1000 + "s");
		long start_read1 = System.currentTimeMillis();
		try {
			bufferedReader = new BufferedReader(new FileReader(mixedFile));
			String temp = null;
			while ((temp = bufferedReader.readLine()) != null) {
				JSONObject jo = new JSONObject(temp);
				long timestamp = jo.getLong("gpstime");
				int timeId = (int) ((timestamp - 1484668800L) / 100);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end_read1 = System.currentTimeMillis();
		System.out.println("[read 1 mixed file] time spent = " + (double) (end_read1 - start_read1) / 1000 + "s");
		long start_split = System.currentTimeMillis();
		ArrayList<SortedMap<Long, List<String>>> list = splitFilesByTimestamp(mixedFile);
		long end_split = System.currentTimeMillis();
		System.out.println("[split 1 mixed file] time spent = " + (double) (end_split - start_split) / 1000 + "s");
		for (int i = 0; i < list.size(); i++) {
			long timeId = 1484668800L + i * 100;
			String fileName = i + "_80000" + "_" + timeId + "100";
			long start_plant1 = System.currentTimeMillis();
			plant(mixedFile.getAbsolutePath().substring(0, mixedFile.getAbsolutePath().lastIndexOf("\\")) + "/"
					+ fileName, list.get(i));
			long end_plant1 = System.currentTimeMillis();
			System.out.println("[plant 1 file] time spent = " + (double) (end_plant1 - start_plant1) / 1000 + "s");
		}
		list.clear();
		File[] files2 = new File[800];
		for (int i = 0; i < 800; i++) {
			files2[i] = new File(file.getAbsolutePath() + "/" + files[i]);
		}
		long start_split800 = System.currentTimeMillis();
		ArrayList<SortedMap<Long, List<String>>> list2 = splitFilesByTimestamp(files2, 100);
		long end_split800 = System.currentTimeMillis();
		System.out.println("[split 800 files] time spent = " + (double) (end_split800 - start_split800) / 1000 + "s");
		for (int i = 0; i < list2.size(); i++) {
			long timeId = 1484668800L + i * 100;
			String fileName = "for800-" + i + "_80000" + "_" + timeId + "100.txt";
			long start_plant1 = System.currentTimeMillis();
			plant(mixedFile.getAbsolutePath().substring(0, mixedFile.getAbsolutePath().lastIndexOf("\\")) + "/"
					+ fileName, list2.get(i));
			long end_plant1 = System.currentTimeMillis();
			System.out.println("[plant 1 file] time spent = " + (double) (end_plant1 - start_plant1) / 1000 + "s");
		}
	}

	private static ArrayList<SortedMap<Long, List<String>>> splitFilesByTimestamp(File file) {
		long initTimeId = 1484668800L;
		int intervalSeconds = 1800;
		ArrayList<SortedMap<Long, List<String>>> list = new ArrayList<SortedMap<Long, List<String>>>();
		for (int i = 0; i < intervalSeconds / 100; i++) {
			SortedMap<Long, List<String>> map = new TreeMap<Long, List<String>>(new Comparator<Long>() {
				public int compare(Long arg0, Long arg1) {
					return (int) (arg0 - arg1);
				}
			});
			list.add(map);
		}
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
			String temp = null;
			while ((temp = bufferedReader.readLine()) != null) {
				JSONObject jo = new JSONObject(temp);
				long timestamp = jo.getLong("gpstime");
				// long startTime = initTimeId;
				int timeId = (int) ((timestamp - initTimeId) / 100);
				addMessage(timestamp, jo.toString(), list.get(timeId));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	private static void addMessage(Long timestamp, final String gpsMessage, SortedMap<Long, List<String>> map) {
		List<String> gpsMessageList = map.getOrDefault(timestamp, new ArrayList<String>());
		gpsMessageList.add(gpsMessage);
		map.put(timestamp, gpsMessageList);
	}

	private static void plant(String fileName, SortedMap<Long, List<String>> sortedMap) {
		File file = new File(fileName);
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
				sortedMap.clear();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("plant messages to " + fileName);
	}

	private static ArrayList<SortedMap<Long, List<String>>> splitFilesByTimestamp(File[] files,
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
		BufferedReader bufferedReader = null;
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
	class Splitor extends Thread{
		public void run(){
			
		}
	}
}
