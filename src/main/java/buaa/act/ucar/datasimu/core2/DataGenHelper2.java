package buaa.act.ucar.datasimu.core2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import net.sf.json.JSONObject;
import buaa.act.ucar.datasimu.ProcStatus;
import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.hbase.Utils;
import buaa.act.ucar.datasimu.zk.ZkUtils;

public class DataGenHelper2 extends Thread {
	private static Logger logger = LogManager.getLogger();
	private static int threadNum = 12;
	private List<DataGenerator2> gpsGeneratorList = new ArrayList<DataGenerator2>(threadNum);
	private List<DataGenerator2> obdGeneratorList = new ArrayList<DataGenerator2>(threadNum);
	private int intervalSeconds;
	private long delaySeconds;
	private int multiples;
	private static AtomicInteger finishNum_gps = new AtomicInteger(0);
	private static AtomicInteger finishNum_obd = new AtomicInteger(0);
	private String jobId;
	private int nodeId = -1;
	private int step;
	private boolean isPlayback=false;

	public DataGenHelper2(String jobId, int nodeId, int step) {
		this.nodeId = nodeId;
		this.jobId = jobId;
		this.step = step;
		finishNum_gps.set(0);
		finishNum_obd.set(0);
		for (int i = 0; i < threadNum; i++) {
			DataGenerator2 generator = new DataGenerator2("gps_" + i);
			gpsGeneratorList.add(generator);
		}
		for (int i = 0; i < threadNum; i++) {
			DataGenerator2 generator = new DataGenerator2("obd_" + i);
			obdGeneratorList.add(generator);
		}
	}

	public void run() {
		startAllOneByOne();
	}

	/**
	 * 由于程序负载很高，gps和obd的数据生成串行，做完一个再做另一个
	 */
	public void startAllOneByOne() {
		int interval = 1 * 1000;
		String nodePath = "/carsimu/jobs" + "/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(nodePath));
		long startTime = Utils.getTimeStamp(JSONObject.fromObject(jo.getString("create")).getString("startTime"))
				+ 1800 * step;
		long targetTime = Utils.getTimeStamp(JSONObject.fromObject(jo.getString("create")).getString("targetTime"))
				+ 1800 * step;
		for (DataGenerator2 generator : obdGeneratorList) {
			generator.setIsPlayback(isPlayback);
			generator.setStartTime(startTime);
			generator.setTargetTime(targetTime);
			generator.setIntervalSeconds(intervalSeconds);
			generator.setMultiples(multiples);
			generator.setDelaySeconds(delaySeconds);
			generator.setRootPath(CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/created");
			generator.start();
		}
		// sleepUntilFinishObdTasks();
		for (DataGenerator2 generator : gpsGeneratorList) {
			generator.setIsPlayback(isPlayback);
			generator.setStartTime(startTime);
			generator.setTargetTime(targetTime);
			generator.setIntervalSeconds(intervalSeconds);
			generator.setMultiples(multiples);
			generator.setDelaySeconds(delaySeconds);
			generator.setRootPath(CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/created");
			generator.start();
		}
		// sleepUntilFinishGpsTasks();
		sleepUntilFinish();
		try {
			InterProcessMutex lock = ZkUtils.lockPath(nodePath);
			ZkUtils.updateCreateStatus(jobId, nodeId, ProcStatus.Created.getExp());
			lock.release();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void addDevList(int groupId, String[] devList) {
		gpsGeneratorList.get(groupId % gpsGeneratorList.size()).tryAddList(groupId, devList);
		obdGeneratorList.get(groupId % obdGeneratorList.size()).tryAddList(groupId, devList);
	}

	public void setId(int id) {
		this.nodeId = id;
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
		DataGenHelper2.threadNum = threadNum;
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
		long interval = 10 * 1000L;
		while (finishNum_gps.get() != threadNum || finishNum_obd.get() != threadNum) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void setIsPlayback(boolean isPlayback2) {
		this.isPlayback = isPlayback2;
	}

}
