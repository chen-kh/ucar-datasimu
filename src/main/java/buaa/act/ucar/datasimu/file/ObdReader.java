package buaa.act.ucar.datasimu.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import buaa.act.ucar.datasimu.core.DataGenerator;
import buaa.act.ucar.datasimu.core.DataProHelper;
import buaa.act.ucar.datasimu.core.Timer;
import buaa.act.ucar.datasimu.hbase.Utils;
import net.sf.json.JSONObject;

public class ObdReader extends Thread {
	private LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>(1);

	public void tryAddFile(File file) {
		try {
			queue.put(file);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		long timeId = 0L;
		while (true) {
			try {
				File file = queue.take();
				long start = System.currentTimeMillis();
				Map<Long, List<String>> map = getDataSetMap(file, timeId);// gps大概花费8s
				long end = System.currentTimeMillis();
				System.out.println("Obd : getDataSetMap from " + file.getName() + " time spent = " + (end - start));
				long baseTimeId = (Long.parseLong(file.getName().split("_")[0]) - 96163200) / 20;
				long intervalSeconds = Long.parseLong(file.getName().split("_")[1]);
				for (timeId = 0 + baseTimeId; timeId < baseTimeId + intervalSeconds / 10; timeId++) {
					// System.out.println("baseTimeId = " + baseTimeId + ",
					// timeId = " + timeId);
					waitUntilAllowToReadNextDataSet(timeId);
					if (map.containsKey(timeId)) {
						List<String> list = map.get(timeId);
						Iterator<String> iterator = list.iterator();
						while (iterator.hasNext()) {
							DataProHelper.addObdMessage2obdProducer(iterator.next());
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private Map<Long, List<String>> getDataSetMap(File file, long baseTimeId) {
		long startTime = (Long.parseLong(file.getName().split("_")[0]) + 96163200) / 2 + Utils.getTimeStamp("2014-01-01 00:00:00");
		Map<Long, List<String>> map = new HashMap<Long, List<String>>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String message = null;
			while ((message = reader.readLine()) != null) {
				JSONObject jo = JSONObject.fromObject(message);
				long timeId = (jo.getLong("gpstime") - startTime) / 10 + baseTimeId;
				List<String> list = map.getOrDefault(timeId, new ArrayList<String>());
				list.add(message);
				map.put(timeId, list);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}

	private void waitUntilAllowToReadNextDataSet(long timeId) {
		Timer timer = Timer.instance;
		final long intervalSeconds = 100;
//		System.out.println("timeId = " + timeId);
		while (timeId > timer.getCurrentTimeId()) {
			try {
				Thread.sleep(intervalSeconds);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
