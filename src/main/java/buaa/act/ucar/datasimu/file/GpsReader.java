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

public class GpsReader extends Thread {
	private static final String KeyNumTimestamp = "0";
	private LinkedBlockingQueue<File> queue = new LinkedBlockingQueue<File>(1);

	public void tryAddFile(File file) {
		try {
			queue.put(file);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while (true) {
			try {
				long timeId = 0L;
				File file = queue.take();
				Map<Long, List<String>> map = getDataSetMap(file);//大概花费8s
				long baseTimeId = (Long.parseLong(file.getName().split("_")[0]) - 26712) * DataGenerator.hourSeconds
						/ 10;
				for (timeId = 0 + baseTimeId; timeId < baseTimeId + DataGenerator.hourSeconds / 10; timeId++) {
					waitUntilAllowToReadNextDataSet(timeId);
					if (map.containsKey(timeId)) {
						List<String> list = map.get(timeId);
						Iterator<String> iterator = list.iterator();
						while (iterator.hasNext()) {
							DataProHelper.addGpsMessage2gpsProducer(iterator.next());
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private Map<Long, List<String>> getDataSetMap(File file) {
		long startTime = Integer.parseInt(file.getName().split("_")[0]) * DataGenerator.hourSeconds
				+ Utils.getTimeStamp("2014-01-01 00:00:00");
		Map<Long, List<String>> map = new HashMap<Long, List<String>>();
		BufferedReader reader = null;
		long baseTimeId = 0L;
		try {
			reader = new BufferedReader(new FileReader(file));
			String message = null;
			while ((message = reader.readLine()) != null) {
				JSONObject jo = JSONObject.fromObject(message);
				long timeId = (jo.getLong(KeyNumTimestamp) - startTime) / 10 + baseTimeId;
				List<String> list = map.getOrDefault(timeId, new ArrayList<String>());
				list.add(message);
				map.put(timeId, list);
			}
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
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
		final long intervalSeconds = 500;
		while (timeId > timer.getCurrentTimeId()) {
			try {
				Thread.sleep(intervalSeconds);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
