package buaa.act.ucar.datasimu.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;

import buaa.act.ucar.datasimu.zk.ZkUtils;

public class DataGenHelper {
	private static int threadNum = 25;
	private List<DataGenerator> gpsGeneratorList = new ArrayList<DataGenerator>(threadNum);
	private List<DataGenerator> obdGeneratorList = new ArrayList<DataGenerator>(threadNum);
	private int id = -1;
	private int intervalSeconds = 3600;
	private long delaySeconds = DataGenerator.hourSeconds * 4;
	private int multiples = 40;
	private static AtomicInteger finishNum_gps = new AtomicInteger(0);
	private static AtomicInteger finishNum_obd = new AtomicInteger(0);
	private static final String statusPath = "/carsimu/devlist/status/";

	public DataGenHelper() {
		finishNum_gps.set(0);
		finishNum_obd.set(0);
		for (int i = 0; i < threadNum; i++) {
			DataGenerator generator = new DataGenerator("gps_" + i);
			gpsGeneratorList.add(generator);
		}
		for (int i = 0; i < threadNum; i++) {
			DataGenerator generator = new DataGenerator("obd_" + i);
			obdGeneratorList.add(generator);
		}
	}

	public void startAll() {
		for (DataGenerator generator : gpsGeneratorList) {
			generator.start();
		}
		for (DataGenerator generator : obdGeneratorList) {
			generator.start();
		}
	}

	/**
	 * 由于程序负载很高，gps和obd的数据生成串行，做完一个再做另一个
	 */
	public void startAllOneByOne() {
		int interval = 1 * 1000;
		JSONObject jo = new JSONObject(ZkUtils.getData(statusPath + id));
		long startTime = jo.getLong("startTime");
		long targetTime = jo.getLong("targetTime");
		for (DataGenerator generator : obdGeneratorList) {
			generator.setStartTime(startTime);
			generator.setTargetTime(targetTime);
			generator.setIntervalSeconds(intervalSeconds);
			generator.setMultiples(multiples);
			generator.setDelaySeconds(delaySeconds);
			generator.start();
			try {
				Thread.sleep(interval);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		sleepUntilFinishObdTasks();
		for (DataGenerator generator : gpsGeneratorList) {
			generator.setStartTime(startTime);
			generator.setTargetTime(targetTime);
			generator.setIntervalSeconds(intervalSeconds);
			generator.setMultiples(multiples);
			generator.setDelaySeconds(delaySeconds);
			generator.start();
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		sleepUntilFinishGpsTasks();
		sleepUntilFinish();
	}

	public void addDevList(int groupId, String[] devList) {
		int index = groupId % threadNum;
		gpsGeneratorList.get(index).tryAddList(groupId, devList);
		obdGeneratorList.get(index).tryAddList(groupId, devList);
	}

	public void setId(int id) {
		this.id = id;
	}

	public static void finishOneGpsGenerator() {
		finishNum_gps.incrementAndGet();
	}

	public static void finishOneObdGenerator() {
		finishNum_obd.incrementAndGet();
	}

	public int getMultiples() {
		return multiples;
	}

	public void setMultiples(int multiples) {
		this.multiples = multiples;
	}

	public long getDelaySeconds() {
		return delaySeconds;
	}

	public void setDelaySeconds(long delaySeconds) {
		this.delaySeconds = delaySeconds;
	}

	public int getIntervalSeconds() {
		return intervalSeconds;
	}

	public void setIntervalSeconds(int intervalSeconds) {
		this.intervalSeconds = intervalSeconds;
	}

	public void setThreadNum(int threadNum) {
		DataGenHelper.threadNum = threadNum;
	}

	private void sleepUntilFinishGpsTasks() {
		long interval = 1000L;
		while (finishNum_gps.get() != threadNum) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sleepUntilFinishObdTasks() {
		long interval = 1000L;
		while (finishNum_obd.get() != threadNum) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sleepUntilFinish() {
		long interval = 1000L;
		while (finishNum_gps.get() != threadNum || finishNum_obd.get() != threadNum) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
