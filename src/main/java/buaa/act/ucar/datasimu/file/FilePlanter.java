package buaa.act.ucar.datasimu.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.LinkedBlockingQueue;

import buaa.act.ucar.datasimu.hbase.KeyValue;

public class FilePlanter {
	private static FileWriter fileWriter;
	private static LinkedBlockingQueue<KeyValue> gpsQueue;
	private static String rootPath;

	public FilePlanter(String rootPath) {
		gpsQueue = new LinkedBlockingQueue<KeyValue>(2);
		FilePlanter.rootPath = rootPath;
	}

	public static void plantMessages(String fileName, List<String> list) {
		for (String msg : list) {
			try {
				fileWriter.write(msg + "\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("plant messages for " + rootPath + "/" + fileName);
	}

	public static void tryAddGpsMessages(String fileName, SortedMap<Long, List<String>> gpsMapKeyedByTimestamp) {
		try {
			gpsQueue.put(new KeyValue(fileName, gpsMapKeyedByTimestamp));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		SortedMap<Long, List<String>> map = null;
		String fileName = null;
		while (true) {
			try {
				KeyValue keyValue = gpsQueue.take();
				fileName = keyValue.getKey();
				map = keyValue.getValue();
				try {
					fileWriter = new FileWriter(new File(rootPath + "/" + fileName));// true表示追加
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Iterator<Entry<Long, List<String>>> iterator = map.entrySet().iterator();
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
	}

}
