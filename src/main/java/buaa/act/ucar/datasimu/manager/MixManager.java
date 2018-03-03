package buaa.act.ucar.datasimu.manager;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.Simulator;
import buaa.act.ucar.datasimu.core.DataMixHelper;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class MixManager extends Thread {
	private Logger logger = LogManager.getLogger();
	private int id = -1;
	private int multiples = 0;
	private int times;

	public MixManager(int id, int multiples) {
		this.id = id;
		this.multiples = multiples;
	}

	public void run() {
		final int carNum4EachGroup = 100;
		final int intervalSeconds_mix1 = 100;
		final int intervalSeconds_mix2 = 10;
		for (int i = 0; i < times; i++) {
			while (!allow2mixData(id)) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (allow2mixData(id)) {
				logger.info("id = " + id + ", begin to mix data ...");
				updateMixedStatus(id, 0);
				mixData(carNum4EachGroup, carNum4EachGroup * 2 * multiples, intervalSeconds_mix1);
				logger.info("id = " + id + ", mixData job on FIRST level done !");
				mixData2(carNum4EachGroup * 2 * multiples, carNum4EachGroup * 2 * multiples * 25, intervalSeconds_mix2);
				logger.info("id = " + id + ", mixData job on SECOND level done !");
				updateMixedStatus(id, 1);
			}
			logger.info("id = " + id + ", mixData job finish 1 time ...");
		}
		updateProducedStatus(id);
	}

	public void setRunTimes(int times) {
		this.times = times;
	}

	private void updateProducedStatus(int id) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("produced", 2);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [prodcuced_status] for " + path + " to [" + 2 + "]");
	}

	private void updateMixedStatus(int id, int status) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("mixed", status);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [mixed_status] for " + path + " to [" + status + "]");
	}

	private boolean allow2mixData(int id) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		int createdStatus = jo.getInt("created");
		if (createdStatus == 1)
			return true;
		return false;
	}

	/**
	 * 混合数据，混合的单位是800辆车，1个小时，也就是说一个文件里面一般包含800辆车在同一个小时的时段内的数据
	 */
	private static void mixData(int carNum4EachOriginalFile, int carNum4EachTargetFile, int intervalSeconds) {
		int group_unit = carNum4EachTargetFile / carNum4EachOriginalFile;
		String obdRootPath = getRealPath() + "/created/obd";
		DataMixHelper dataMixHelper = new DataMixHelper(1, getRealPath() + "/mixed1/", intervalSeconds);
		int multiples;
		multiples = new File(obdRootPath).list().length / 50;
		if (DataMixHelper.checkFileSetAndPrepare(obdRootPath, group_unit, multiples)) {// 这里的400其实是multiples
			File file = new File(obdRootPath);
			String[] fileList = file.list();
			for (int i = 0; i < fileList.length; i += group_unit) {
				File[] files = new File[group_unit];
				for (int j = 0; j < group_unit; j++) {
					files[j] = new File(obdRootPath + "/" + fileList[i + j]);
				}
				dataMixHelper.addObdFileGroup(files);
			}
		}
		String gpsRootPath = getRealPath() + "/created/gps";
		multiples = new File(obdRootPath).list().length / 50;
		if (DataMixHelper.checkFileSetAndPrepare(gpsRootPath, group_unit, multiples)) {
			File file = new File(gpsRootPath);
			String[] fileList = file.list();
			for (int i = 0; i < fileList.length; i += group_unit) {
				File[] files = new File[group_unit];
				for (int j = 0; j < group_unit; j++) {
					files[j] = new File(gpsRootPath + "/" + fileList[i + j]);
				}
				dataMixHelper.addGpsFileGroup(files);
			}
		}
		dataMixHelper.startALlOneByOne1();
	}

	/**
	 * 混合数据，混合的单位是800辆车，1个小时，也就是说一个文件里面一般包含800辆车在同一个小时的时段内的数据
	 */
	private static void mixData2(int carNum4EachOriginalFile, int carNum4EachTargetFile, int intervalSeconds) {
		int group_unit = carNum4EachTargetFile / carNum4EachOriginalFile;
		String obdRootPath = getRealPath() + "/mixed1/obd";
		DataMixHelper dataMixHelper = new DataMixHelper(1, getRealPath() + "/mixed2/", intervalSeconds);
		if (DataMixHelper.checkFileSetAndPrepare(obdRootPath, group_unit, 1)) {
			File file = new File(obdRootPath);
			String[] fileList = file.list();
			for (int i = 0; i < fileList.length; i += group_unit) {
				File[] files = new File[group_unit];
				for (int j = 0; j < group_unit; j++) {
					files[j] = new File(obdRootPath + "/" + fileList[i + j]);
				}
				dataMixHelper.addObdFileGroupByTimeId(files);
			}
		}
		String gpsRootPath = getRealPath() + "/mixed1/gps";
		if (DataMixHelper.checkFileSetAndPrepare(gpsRootPath, group_unit, 1)) {
			File file = new File(gpsRootPath);
			String[] fileList = file.list();
			for (int i = 0; i < fileList.length; i += group_unit) {
				File[] files = new File[group_unit];
				for (int j = 0; j < group_unit; j++) {
					files[j] = new File(gpsRootPath + "/" + fileList[i + j]);
				}
				dataMixHelper.addGpsFileGroupByTimeId(files);
			}
		}
		dataMixHelper.startALlOneByOne2();
	}

	private static String getRealPath() {
		String rootPath = Simulator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}

}
