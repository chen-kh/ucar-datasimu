package buaa.act.ucar.datasimu.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.core.DataGenHelper;
import buaa.act.ucar.datasimu.core.DataGenerator;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class CreateManager extends Thread {
	private Logger logger = LogManager.getLogger();
	private int id;
	private int multiples;
	private String[] totalDevList;
	private long totalIntervalSeconds;

	public CreateManager(int id, int multiples) {
		this.id = id;
		this.multiples = multiples;
	}

	public void run() {
		int times = (int) (totalIntervalSeconds / 1800);
		for (int i = 0; i < times; i++) {
			final int carNum4EachGroup = 100;
			final int intervalSeconds4EachCar = 1800;// 每次获取1800s的数据
			final long delaySeconds = DataGenerator.hourSeconds * 1;
			while (!allow2createNextDataSet(id)) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("id = " + id + ", begin to create data file set ...");
			updateCreatedStatus(id, 0);// 设置状态为正在产生数据
			createData(id, totalDevList, carNum4EachGroup, intervalSeconds4EachCar, multiples, delaySeconds);
			logger.info("id = " + id + ", create data job finish 1 time !");
			updateCreatedStatus(id, 1);
			while (!allow2updateTime(id)) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("id = " + id + ", begin to update time and init_status on zk for next time to create data ...");
			updateTimeAndInitStatus(id);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void setTotalDevList(String[] totalDevList) {
		this.totalDevList = totalDevList;
	}

	public void setTotalIntervalSeconds(long intervalSeconds) {
		this.totalIntervalSeconds = intervalSeconds;
	}

	private void updateTimeAndInitStatus(int id) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = new JSONObject();
		jo.put("startTime", jo.getLong("startTime") + jo.getLong("intervalSeconds"));
		jo.put("targetTime", jo.getLong("targetTime") + jo.getLong("intervalSeconds"));
		jo.put("intervalSeconds", 1800);
		jo.put("created", -1);
		jo.put("mixed", -1);
		jo.put("produced", -1);
		String value = jo.toString();
		ZkUtils.setData(path, value);
		logger.info("zk client updated [time and init_status] for " + path + " to ["
				+ (jo.getLong("startTime") + jo.getLong("intervalSeconds")) + ","
				+ (jo.getLong("targetTime") + jo.getLong("intervalSeconds"))
				+ "\tcreated_status = -1, mixed_status = -1, produced_status = -1 " + "]");
	}

	private boolean allow2updateTime(int id) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		int createdStatus = jo.getInt("created");
		if (createdStatus == 2) {
			return true;
		}
		return false;
	}

	private void updateCreatedStatus(int id, int status) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("created", status);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [created_status] for " + path + " to [" + status + "]");
	}

	private boolean allow2createNextDataSet(int id) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		int mixedStatus = jo.getInt("mixed");
		int createdStatus = jo.getInt("created");
		int producedStatus = jo.getInt("produced");
		if (createdStatus == -1 && mixedStatus == -1 && producedStatus == -1) {
			return true;
		}
		if (mixedStatus == 1) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @param devListId
	 * @param totalDevList
	 * @param carNum4EachGroup
	 * @param intervalSeconds4EachCar
	 * @param multiples
	 * @param delaySeconds
	 */
	private static void createData(int devListId, String[] totalDevList, int carNum4EachGroup,
			int intervalSeconds4EachCar, int multiples, long delaySeconds) {
		int group_unit = carNum4EachGroup;
		// 开启所有产生数据线程
		DataGenHelper dataGenHelper = new DataGenHelper();
		dataGenHelper.setId(devListId);
		dataGenHelper.setIntervalSeconds(intervalSeconds4EachCar);
		dataGenHelper.setMultiples(multiples);
		dataGenHelper.setDelaySeconds(delaySeconds);
		for (int i = 0; i < totalDevList.length; i += group_unit) {
			String[] devList = new String[group_unit];
			for (int j = 0; j < devList.length; j++) {
				devList[j] = totalDevList[i + j];
			}
			if (devList != null && devList.length > 0) {
				int groupId = devListId * (totalDevList.length / group_unit) + i / group_unit;
				dataGenHelper.addDevList(groupId, devList);
			}
		}
		dataGenHelper.startAllOneByOne();
	}
}
