package buaa.act.ucar.datasimu;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.core.Timer2;
import buaa.act.ucar.datasimu.core2.DataGenHelper2;
import buaa.act.ucar.datasimu.core2.DataGenHelper4large;
import buaa.act.ucar.datasimu.core2.DataGenerator2;
import buaa.act.ucar.datasimu.core2.DataMixHelper2;
import buaa.act.ucar.datasimu.core2.DataProHelper2;
import buaa.act.ucar.datasimu.file.DataReader;
import buaa.act.ucar.datasimu.hbase.Utils;
import buaa.act.ucar.datasimu.zk.ZkClient;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

@Deprecated
public class Simulator2 {
	private static Logger logger = LogManager.getLogger();
	private static int id = -1;

	/**
	 * 参数解释：
	 * <p>
	 * args数组长度为5，从输入获取，依次命名为dateSource，dateTarget，intervalSeconds,carsimuNumber
	 * ,methodNum
	 * <p>
	 * dateSource,dateTarget: 分别指从什么时间开始获取历史数据，从什么时间开始生成模拟数据。格式为 yyyy-MM-dd
	 * hh:mm:ss
	 * <p>
	 * intervalSeconds：程序一次性从HBase查询的消息的时间间隔，一般时间越长数量越多
	 * <p>
	 * carsimuNumber：模拟车辆数.由于所有数据都是从2.5w辆车里面获取的数据，所以这个数量必须是25000的整数倍
	 * methodNum：生成数据的方法，有两种，详细见文档
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		initDir();
		if (args != null && args.length == 5) {
			long dateSource = Utils.getTimeStamp(args[0]);
			long dateTarget = Utils.getTimeStamp(args[1]);
			long intervalSeconds = Long.parseLong(args[2]);// 必须是1800的倍数
			int carsimuNumber = Integer.parseInt(args[3]);// 必须是25000的整数倍
			int methodNum = Integer.parseInt(args[4]);
			String jobId = args[5];
			ZkClient zkClient = new ZkClient();
			zkClient.start();
			String[] totalDevList = getIdAndDevList(zkClient.client, jobId);
			int id = Simulator2.id;
			int steps = (int) (intervalSeconds / 1800);
			initZkDir(zkClient.client, jobId, id);
			initJobStatus(zkClient.client, jobId, id, intervalSeconds, steps, args[0], args[1]);
			int multiples = (int) (carsimuNumber / 25000);
			// stage create and then stage mix
			int carNum4EachGroup = 100;
			int intervalSeconds4EachCar = 1800;// 每次获取1800s的数据
			long delaySeconds = 0L;
			if (methodNum == 1)
				delaySeconds = DataGenerator2.hourSeconds * 1;
			else if (methodNum == 2)
				delaySeconds = 2L;
			else {
				System.out.println("methodNumber shoule be chosen from [1, 2]");
				System.exit(0);
			}
			int intervalSeconds_mix = 10;
			for (int i = 0; i < steps; i++) {
				logger.info("==================================================");
				logger.info("id = " + id + ", begin to create data file set ...");
				updateCreatedStatus(id, 0);// 设置状态为正在产生数据
				createData(methodNum, totalDevList, carNum4EachGroup, intervalSeconds4EachCar, multiples, delaySeconds,
						jobId, id, i);
				logger.info("id = " + id + ", create data job finish " + i + "/" + steps);
				updateCreatedStatus(id, 1);
				logger.info("--------------------------------------------------");
				logger.info(
						"id = " + id + ", begin to update time and init_status on zk for next time to create data ...");
				updateTimeAndInitStatus(id);
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				logger.info("--------------------------------------------------");
				logger.info("id = " + id + ", begin to mix data ...");
				updateMixedStatus(id, 0);
				int mixThreadNum = 5;
				mixData(totalDevList.length * multiples, totalDevList.length * multiples, intervalSeconds_mix,
						mixThreadNum, jobId, id, i);
				logger.info("id = " + id + ", mixData job done !");
				logger.info("--------------------------------------------------");
				updateMixedStatus(id, 1);
				logger.info("id = " + id + ", mixData job finish " + i + "/" + steps);
				logger.info("==================================================");
			}
			updateProducedStatus(id, 2);
			produceData();
		} else {
			System.out.println("Usage: <dateSource> <dateTarget> <intervalSeconds> <carsimuNumber> <methodNumber>");
			System.out.println(
					"<methodNumber> means which method to generate data, 1 for long delaySeconds(for not very big carsimuNumber < 300w), 2 for small delaySeconds(for very big carsimuNumber > 500w)");
			logger.error(
					"no input arguments, should be : <dateSource> <dateTarget> <intervalSeconds> <carsimuNumber> <methodNumber>");
		}

	}

	private static void produceData() {
		String rootPath = getRealPath() + "/mixed/";
		long initTimeId = Long.parseLong(new File(rootPath + "gps").list()[0].split("_")[0]) - 20L;
		// long initTimeId = System.currentTimeMillis()/1000;
		Timer2.instance.setInitTimeId(initTimeId);
		Timer2.instance.start();
		DataProHelper2.setThreadNum(8);
		DataProHelper2.initProducers();
		DataProHelper2.startAll();
		String[] gpsFileNames = new File(rootPath + "/gps").list();
		String[] obdFileNames = new File(rootPath + "/obd").list();
		File[] gpsFiles = new File[gpsFileNames.length];
		File[] obdFiles = new File[obdFileNames.length];
		for(int i = 0; i< gpsFiles.length;i++){
			gpsFiles[i] = new File(rootPath + "/" + gpsFileNames[i]);
			obdFiles[i] = new File(rootPath + "/" + obdFileNames[i]);
		}
		new DataReader("gps", gpsFiles).start();
		new DataReader("obd", obdFiles).start();
	}

	private static void updateCreatedStatus(int id, int status) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("created", status);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [created_status] for " + path + " to [" + status + "]");
	}

	private static void updateTimeAndInitStatus(int id) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
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

	private static void updateProducedStatus(int id, int status) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("produced", 2);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [prodcuced_status] for " + path + " to [" + status + "]");
	}

	private static void updateMixedStatus(int id, int status) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("mixed", status);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [mixed_status] for " + path + " to [" + status + "]");
	}

	private static String getRealPath() {
		String rootPath = Simulator2.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}

	private static void initDir() {
		String rootPath = getRealPath();
		File createdFile = new File(rootPath + "/created");
		File createdGpsFile = new File(rootPath + "/created/gps");
		File createdObdFile = new File(rootPath + "/created/obd");
		File mixedFile = new File(rootPath + "/mixed");
		File mixedGpsFile = new File(rootPath + "/mixed/gps");
		File mixedObdFile = new File(rootPath + "/mixed/obd");
		if (!createdFile.exists())
			createdFile.mkdir();
		if (!createdGpsFile.exists())
			createdGpsFile.mkdir();
		if (!createdObdFile.exists())
			createdObdFile.mkdir();
		if (!mixedFile.exists())
			mixedFile.mkdir();
		if (!mixedGpsFile.exists())
			mixedGpsFile.mkdir();
		if (!mixedObdFile.exists())
			mixedObdFile.mkdir();
	}

	private static void initZkDir(CuratorFramework opter, String jobId, int nodeId) {
		String jobPath = "/carsimu/jobs/" + jobId;
		String nodePath = "/carsimu/jobs/" + jobId + "/" + nodeId;
		String createTaskPath = nodePath + "/create";
		String mixTaskPath = nodePath + "/mix";
		String produceTaskPath = nodePath + "/produce";
		String createInitInfo = "c";
		String mixInitInfo = "m";
		String produceInitInfo = "p";
		try {
			if (opter.checkExists().forPath(nodePath) == null) {
				opter.create().withMode(CreateMode.PERSISTENT).forPath(jobPath, jobId.getBytes());
				opter.create().withMode(CreateMode.PERSISTENT).forPath(nodePath, (nodeId + "").getBytes());
				opter.create().withMode(CreateMode.PERSISTENT).forPath(createTaskPath, createInitInfo.getBytes());
				opter.create().withMode(CreateMode.PERSISTENT).forPath(mixTaskPath, mixInitInfo.getBytes());
				opter.create().withMode(CreateMode.PERSISTENT).forPath(produceTaskPath, produceInitInfo.getBytes());
				logger.info("init for jobId = " + jobId + ", nodeId = " + nodeId + " done!");
			} else {
				logger.info("path = " + nodePath + " already exists!");
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private static String[] getIdAndDevList(CuratorFramework client, String jobId) {
		CuratorFramework opter = client;
		String devDataPath = "/carsimu/devlist/data";
		String nodeInfoPath = "/carsimu/jobs/nodeInfo";
		int id = -1;
		String[] totalDevList = null;
		// 获取一个大集合里面的车辆集合
		try {
			InterProcessMutex lock = new InterProcessMutex(opter, nodeInfoPath);
			while (!lock.acquire(10, TimeUnit.SECONDS)) {
				Thread.sleep(1000);
				System.out.println(Thread.currentThread().getName() + " is waiting for lock of " + nodeInfoPath);
			}
			String info = new String(opter.getData().forPath(nodeInfoPath));
			JSONObject jo = JSONObject.fromObject(info);
			if (jo.containsKey(jobId)) {
				for (int i = 0; i < 4; i++) {
					String jobNode = jo.getString(jobId);
					if (!jobNode.contains(i + "")) {
						id = i;
					}
				}
				if (id == -1) {
					logger.info("there are already 5 node for jobId = " + jobId);
					logger.info("prepare to exit");
					System.exit(0);
				}
			} else {
				id = 0;
			}
			lock.release();
			totalDevList = new String(opter.getData().forPath(devDataPath + "/" + id)).split("\\|");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("something wrong when get id of big_group");
			if (id == -1) {
				System.exit(0);
			}
		}
		Simulator2.id = id;
		return totalDevList;
	}

	private static void initJobStatus(CuratorFramework client, String jobId, int nodeId, long intervalTime, int steps,
			String dateSource, String dateTarget) {
		String nodePath = "/carsimu/jobs/" + jobId + "/" + nodeId;
		String createTaskPath = nodePath + "/create";
		String mixTaskPath = nodePath + "/mix";
		String produceTaskPath = nodePath + "/produce";
		JSONObject c_jo = new JSONObject();
		c_jo.put("status", "prepare");
		c_jo.put("progress", "0/" + steps);
		c_jo.put("startTime", dateSource);
		c_jo.put("targetTime", dateTarget);
		c_jo.put("intervalSecondsUnit", "1800");
		c_jo.put("intervalSecondsTotal", intervalTime);
		JSONObject m_jo = new JSONObject();
		m_jo.put("status", "wait");
		m_jo.put("progress", "0/" + steps);
		m_jo.put("startTime", dateSource);
		m_jo.put("targetTime", dateTarget);
		m_jo.put("intervalSecondsUnit", "1800");
		m_jo.put("intervalSecondsTotal", intervalTime);
		JSONObject p_jo = new JSONObject();
		p_jo.put("status", "wait");
		p_jo.put("progress", "0/" + steps);
		p_jo.put("startTime", dateSource);
		p_jo.put("targetTime", dateTarget);
		p_jo.put("intervalSecondsUnit", "1800");
		p_jo.put("intervalSecondsTotal", intervalTime);
		String createInitInfo = c_jo.toString();
		String mixInitInfo = m_jo.toString();
		String produceInitInfo = p_jo.toString();
		try {
			client.setData().forPath(createTaskPath, createInitInfo.getBytes());
			client.setData().forPath(mixTaskPath, mixInitInfo.getBytes());
			client.setData().forPath(produceTaskPath, produceInitInfo.getBytes());

		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private static void createData(int method_num, String[] totalDevList, int carNum4EachGroup,
			int intervalSeconds4EachCar, int multiples, long delaySeconds, String jobId, int nodeId, int step) {
		int group_unit = carNum4EachGroup;
		// 开启所有产生数据线程
		if (method_num == 1) {
			DataGenHelper2 dataGenHelper = new DataGenHelper2(jobId, nodeId, step);
			dataGenHelper.setId(nodeId);
			dataGenHelper.setIntervalSeconds(intervalSeconds4EachCar);
			dataGenHelper.setMultiples(multiples);
			dataGenHelper.setDelaySeconds(delaySeconds);
			for (int i = 0; i < totalDevList.length; i += group_unit) {
				String[] devList = new String[group_unit];
				for (int j = 0; j < devList.length; j++) {
					devList[j] = totalDevList[i + j];
				}
				if (devList != null && devList.length > 0) {
					int groupId = nodeId * (totalDevList.length / group_unit) + i / group_unit;
					dataGenHelper.addDevList(groupId, devList);
				}
			}
			// dataGenHelper.startAllOneByOne();
			dataGenHelper.start();
		}
		if (method_num == 2) {
			DataGenHelper4large dataGenHelper = new DataGenHelper4large(jobId, nodeId, step);
			dataGenHelper.setId(nodeId);
			dataGenHelper.setIntervalSeconds(intervalSeconds4EachCar);
			dataGenHelper.setMultiples(multiples);
			dataGenHelper.setDelaySeconds(delaySeconds);
			for (int i = 0; i < totalDevList.length; i += group_unit) {
				String[] devList = new String[group_unit];
				for (int j = 0; j < devList.length; j++) {
					devList[j] = totalDevList[i + j];
				}
				if (devList != null && devList.length > 0) {
					int groupId = nodeId * (totalDevList.length / group_unit) + i / group_unit;
					dataGenHelper.addDevList(groupId, devList);
				}
			}
			// dataGenHelper.startAllOneByOne();
			dataGenHelper.start();
		}
	}

	private static void mixData(int carNum4EachOriginalFile, int carNum4EachTargetFile, int intervalSeconds,
			int threadNum, String jobId, int nodeId, int step) {
		int group_unit = carNum4EachTargetFile / carNum4EachOriginalFile;
		DataMixHelper2 dataMixHelper = new DataMixHelper2(jobId, nodeId, step, threadNum);
		String obdRootPath = CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/created/obd";
		if (DataMixHelper2.checkFileSetAndPrepare(obdRootPath, group_unit, 1)) {
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
		String gpsRootPath = CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/created/gps";
		if (DataMixHelper2.checkFileSetAndPrepare(gpsRootPath, group_unit, 1)) {
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
		dataMixHelper.startALlOneByOne();
	}
}