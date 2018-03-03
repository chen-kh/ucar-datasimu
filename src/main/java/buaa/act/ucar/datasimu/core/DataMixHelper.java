package buaa.act.ucar.datasimu.core;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.Simulator;

public class DataMixHelper {
	private static Logger logger = LogManager.getLogger();
	private int threadNum = 1;
	private List<DataMixer> gpsMixers = new ArrayList<DataMixer>();
	private List<DataMixer> obdMixers = new ArrayList<DataMixer>();
	private static int min_groupId = 0;
	private static int min_parentGroupId = 0;
	private int intervalSeconds = 100;
	private String rootPath = "";// 文件写入目录
	public static AtomicInteger gpsMixerFinishNum = new AtomicInteger(0);
	public static AtomicInteger obdMixerFinishNum = new AtomicInteger(0);

	public DataMixHelper(int threadNum, String rootPath, int intervalSeconds) {
		this.threadNum = threadNum;
		this.rootPath = rootPath;
		gpsMixerFinishNum.set(0);
		obdMixerFinishNum.set(0);
		for (int i = 0; i < threadNum; i++) {
			DataMixer mixer = new DataMixer("gps_" + i, rootPath, intervalSeconds);
			gpsMixers.add(mixer);
		}
		for (int i = 0; i < threadNum; i++) {
			DataMixer mixer = new DataMixer("obd_" + i, rootPath, intervalSeconds);
			obdMixers.add(mixer);
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
		File file = new File(path);
		String[] fileList = file.list();
		String[] infos = fileList[0].split("_");
		int initGroupId = Integer.parseInt(infos[2]);
		setMin_parentGroupId(initGroupId);
		setMin_groupId(initGroupId / (group_unit / multiples));
		if (fileList.length % group_unit != 0) {
			System.out.println("create的数据文件集数量异常");
			return false;
		}
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
		gpsMixers.get((int) (timeId % threadNum)).tryAddFileGroup((int) timeId, files);
	}

	public void addObdFileGroupByTimeId(File[] files) {
		long timeId = Long.parseLong(files[0].getName().split("_")[0]);
		obdMixers.get((int) (timeId % threadNum)).tryAddFileGroup((int) timeId, files);
	}

	public static void setMin_groupId(int min_groupId) {
		DataMixHelper.min_groupId = min_groupId;
	}

	public static void setMin_parentGroupId(int min_parentGroupId) {
		DataMixHelper.min_parentGroupId = min_parentGroupId;
	}

	public void setIntervalSeconds(int intervalSeconds) {
		this.intervalSeconds = intervalSeconds;
	}

	public void startALlOneByOne1() {
		for (DataMixer mixer : obdMixers) {
			mixer.start();
		}
		sleepUntilFinishAllObdTasks();
		deleteAllObdFiles(getRealPath() + "/created/obd");
		for (DataMixer mixer : gpsMixers) {
			mixer.start();
		}
		sleepUntilFinishAllGpsTasks();
		deleteAllGpsFiles(getRealPath() + "/created/gps");
	}

	public void startALlOneByOne2() {
		for (DataMixer mixer : obdMixers) {
			mixer.start();
		}
		sleepUntilFinishAllObdTasks();
		deleteAllObdFiles(getRealPath() + "/mixed1/obd");
		for (DataMixer mixer : gpsMixers) {
			mixer.start();
		}
		sleepUntilFinishAllGpsTasks();
		deleteAllGpsFiles(getRealPath() + "/mixed1/gps");
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

	private static String getRealPath() {
		String rootPath = Simulator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		System.out.println(rootPath);
		return rootPath;
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
