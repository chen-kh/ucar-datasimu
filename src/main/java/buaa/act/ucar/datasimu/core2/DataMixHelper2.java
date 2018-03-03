package buaa.act.ucar.datasimu.core2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.ProcStatus;
import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.zk.ZkUtils;

public class DataMixHelper2 extends Thread {
	private static Logger logger = LogManager.getLogger();
	private int threadNum = 1;
	private List<DataMixer2> gpsMixers = new ArrayList<DataMixer2>();
	private List<DataMixer2> obdMixers = new ArrayList<DataMixer2>();
	private static int min_groupId = 0;
	private static int min_parentGroupId = 0;
	private int intervalSeconds = 10;
	private String jobId;
	private int nodeId;
	private int step;
	public static AtomicInteger gpsMixerFinishNum = new AtomicInteger(0);
	public static AtomicInteger obdMixerFinishNum = new AtomicInteger(0);

	public DataMixHelper2(String jobId, int nodeId, int step, int threadNum) {
		this.jobId = jobId;
		this.nodeId = nodeId;
		this.step = step;
		this.threadNum = threadNum;
		gpsMixerFinishNum.set(0);
		obdMixerFinishNum.set(0);
		for (int i = 0; i < threadNum; i++) {
			DataMixer2 mixer = new DataMixer2("gps_" + i, intervalSeconds);
			mixer.setRootPath(CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/mixed");
			gpsMixers.add(mixer);
		}
		for (int i = 0; i < threadNum; i++) {
			DataMixer2 mixer = new DataMixer2("obd_" + i, intervalSeconds);
			mixer.setRootPath(CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/mixed");
			obdMixers.add(mixer);
		}
	}

	public void run() {
		startALlOneByOne();
	}

	public void startALlOneByOne() {
		for (DataMixer2 mixer : obdMixers) {
			mixer.start();
		}
		for (DataMixer2 mixer : gpsMixers) {
			mixer.start();
		}
		sleepUntilFinishAllGpsTasks();
		sleepUntilFinishAllObdTasks();
		deleteAllGpsFiles(CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/created/gps");
		deleteAllObdFiles(CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/created/obd");
		try {
			InterProcessMutex lock = ZkUtils.lockPath("/carsimu/jobs/" + jobId + "/" + nodeId);
			ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Mixed.getExp());
			lock.release();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 检查文件集是否正常，检查项包括 1，所有的文件是否是一个时间段的数据 2，文件集内的文件数量是否是8的倍数
	 * 
	 * @param path
	 * @param group_unit
	 * @return
	 */
	public static boolean checkFileSetAndPrepare(String path, int group_unit, int multiples) {
		// File file = new File(path);
		// String[] fileList = file.list();
		// String[] infos = fileList[0].split("_");
		// int initGroupId = Integer.parseInt(infos[2]);
		// setMin_parentGroupId(initGroupId);
		// setMin_groupId(initGroupId / (group_unit / multiples));
		// if (fileList.length % group_unit != 0) {
		// System.out.println("create的数据文件集数量异常");
		// return false;
		// }
		return true;
	}

	public void addGpsFileGroup(File[] files) {
		int groupId = (Integer.parseInt(files[0].getName().split("_")[2]) - min_parentGroupId) / 2 + min_groupId;
		gpsMixers.get(groupId % threadNum).tryAddFileGroup(groupId, files);
	}

	public void addObdFileGroup(File[] files) {
		int groupId = (Integer.parseInt(files[0].getName().split("_")[2]) - min_parentGroupId) / 2 + min_groupId;
		obdMixers.get(groupId % threadNum).tryAddFileGroup(groupId, files);
	}

	public void addGpsFileGroupByTimeId(File[] files) {
		long timeId = Long.parseLong(files[0].getName().split("_")[0]);
		gpsMixers.get((int) (Math.abs(files.hashCode()) % threadNum)).tryAddFileGroup((int) timeId, files);
	}

	public void addObdFileGroupByTimeId(File[] files) {
		long timeId = Long.parseLong(files[0].getName().split("_")[0]);
		obdMixers.get((int) (Math.abs(files.hashCode()) % threadNum)).tryAddFileGroup((int) timeId, files);
	}

	public static void setMin_groupId(int min_groupId) {
		DataMixHelper2.min_groupId = min_groupId;
	}

	public static void setMin_parentGroupId(int min_parentGroupId) {
		DataMixHelper2.min_parentGroupId = min_parentGroupId;
	}

	public void setIntervalSeconds(int intervalSeconds) {
		this.intervalSeconds = intervalSeconds;
	}

	private static void deleteAllGpsFiles(String path) {
		String[] files = new File(path).list();
		logger.info("begin to delete all gps files in " + path);
		logger.info("total gps file number = " + files.length);
		for (String file : files) {
			new File(path + "/" + file).delete();
		}
		logger.info("delete all gps files done !");
	}

	private static void deleteAllObdFiles(String path) {
		String[] files = new File(path).list();
		logger.info("begin to delete all obd files in " + path);
		logger.info("total obd files number = " + files.length);
		for (String file : files) {
			new File(path + "/" + file).delete();
		}
		logger.info("delete all obd files done !");

	}

	private void sleepUntilFinishAllGpsTasks() {
		while (gpsMixerFinishNum.get() != threadNum) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sleepUntilFinishAllObdTasks() {
		while (obdMixerFinishNum.get() != threadNum) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
