package buaa.act.ucar.datasimu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.core2.DataGenerator2;
import buaa.act.ucar.datasimu.obspat.CreateObserver;
import buaa.act.ucar.datasimu.obspat.MixObserver;
import buaa.act.ucar.datasimu.obspat.ProduceObserver;
import buaa.act.ucar.datasimu.obspat.Subject;
import buaa.act.ucar.datasimu.zk.ZkClient;
import buaa.act.ucar.datasimu.zk.ZkWatcher;
import net.sf.json.JSONObject;

/*
 * 跟模拟数据不同，此类的目的是回放数据。车辆id从文件中获取。
 * 
 */
public class Playback {

	private static Logger logger = LogManager.getLogger();
	private static int nodeId = -1;
	private static Subject subject = new Subject();

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
		args = new String[] { "2016-06-01 12:00:00", "2018-02-01 19:00:00", "7200", "1", "test-playback-2" };
		if (args != null && args.length == 5) {
			/*
			 * 获取输入数据
			 */
			String dateSource = args[0];
			String dateTarget = args[1];
			long intervalSeconds = Long.parseLong(args[2]);// 必须是1800的倍数
			int methodNum = Integer.parseInt(args[3]);
			String jobId = args[4];
			/*
			 * 从zookeeper获取工作节点id和devList，并初始化Job节点和节点下的信息
			 */
			ZkClient zkClient = new ZkClient();
			zkClient.start();
			String[] totalDevList = getIdAndDevList(zkClient.client, jobId);
			int id = Playback.nodeId;
			int steps = (int) (intervalSeconds / 1800);
			initDir(jobId, nodeId, steps);
			initZkDir(zkClient.client, jobId, id);
			initJobStatus(zkClient.client, jobId, id, intervalSeconds, steps, dateSource, dateTarget);
			zkClient.close();
			/*
			 * 配置程序运行参数
			 */
			int carsimuNumber = totalDevList.length;// 必须是25000的整数倍
			int multiples = 1;
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
			int mixThreadNum = 5;
			/*
			 * 添加观察者
			 */
			subject.setJobId(jobId);
			subject.setNodeId(nodeId);
			CreateObserver createObserver = new CreateObserver(subject);
			MixObserver mixObserver = new MixObserver(subject);
			ProduceObserver produceObserver = new ProduceObserver(subject);
			createObserver.setCreatePara(methodNum, totalDevList, carNum4EachGroup, intervalSeconds4EachCar, multiples,
					delaySeconds, jobId, id);
			createObserver.setIsPlayback();
			mixObserver.setMixPara(totalDevList, multiples, intervalSeconds_mix, mixThreadNum, jobId, nodeId);
			produceObserver.setProdPara(jobId, id);
			/*
			 * ZkWatcher开始监听
			 */
			ZkWatcher watcher = new ZkWatcher(subject, jobId, nodeId);
			watcher.start();
		} else {
			System.out.println(
					"Usage: <dateSource> <dateTarget> <intervalSeconds> <methodNumber> <jobId>");
			System.out.println(
					"<methodNumber> means which method to generate data, 1 for long delaySeconds(for not very big carsimuNumber < 300w), 2 for small delaySeconds(for very big carsimuNumber > 500w)");
			logger.error(
					"no input arguments, should be : <dateSource> <dateTarget> <intervalSeconds> <methodNumber>");
		}
	
	}

	private static void initDir(String jobId, int nodeId, int steps) {
		String rootPath = CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId;
		File roorDir = new File(rootPath);
		if (!roorDir.exists()) {
			roorDir.mkdirs();
			for (int i = 0; i < steps; i++) {
				String stepPath = rootPath + "/" + i;
				File stepFile = new File(stepPath);
				File createdFile = new File(stepPath + "/created");
				File createdGpsFile = new File(stepPath + "/created/gps");
				File createdObdFile = new File(stepPath + "/created/obd");
				File mixedFile = new File(stepPath + "/mixed");
				File mixedGpsFile = new File(stepPath + "/mixed/gps");
				File mixedObdFile = new File(stepPath + "/mixed/obd");
				if (!stepFile.exists())
					stepFile.mkdir();
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
		} else {
			logger.error("the rootDir = " + rootPath + " already exists!");
		}
	}

	private static void initZkDir(CuratorFramework opter, String jobId, int nodeId) {
		String jobPath = "/carsimu/jobs/" + jobId;
		String nodePath = "/carsimu/jobs/" + jobId + "/" + nodeId;
		try {
			if (opter.checkExists().forPath(jobPath) == null) {
				opter.create().withMode(CreateMode.PERSISTENT).forPath(jobPath, jobId.getBytes());
			} else {
				logger.info("jobId = " + jobId + ", path = " + jobPath + " already exists!");
			}
			if (opter.checkExists().forPath(nodePath) == null) {
				opter.create().withMode(CreateMode.PERSISTENT).forPath(nodePath, (nodeId + "").getBytes());
				logger.info("init for jobId = " + jobId + ", nodeId = " + nodeId + " done!");
			} else {
				logger.info("nodeId = " + nodeId + ", path = " + nodePath + " already exists!");
				logger.error("nodeId = " + nodeId + ", path = " + nodePath + " already exists!\nexit 1");
				System.exit(1);
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
						jobNode += (id + ";");
						jo.put(jobId, jobNode);
						break;
					}
				}
				if (id == -1) {
					logger.info("there are already 5 node for jobId = " + jobId);
					logger.info("prepare to exit");
					System.exit(1);
				}
			} else {
				logger.info("jobId = " + jobId + " not in path = " + nodeInfoPath);
				logger.info("add jobId = " + jobId + " to path = " + nodeInfoPath);
				jo.put(jobId, "0;");
				id = 0;
			}
			opter.setData().forPath(nodeInfoPath, jo.toString().getBytes());
			lock.release();
			totalDevList = getDevList(getRealPath() + "/dsn.txt");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("something wrong when get id of big_group");
			if (id == -1) {
				System.exit(0);
			}
		}
		Playback.nodeId = id;
		return totalDevList;
	}
	private static String[] getDevList(String devsFilePath) throws IOException {
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(new File(devsFilePath)));
		StringBuffer devs = new StringBuffer();
		String devicesn = null;
		while((devicesn = reader.readLine()) != null){
			devs.append(devicesn + ",");
		}
		reader.close();
		return devs.toString().substring(0, devs.length()-1).split(",");
	}
	private static void initJobStatus(CuratorFramework client, String jobId, int nodeId, long intervalTime, int steps,
			String dateSource, String dateTarget) {
		String nodePath = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject c_jo = new JSONObject();
		c_jo.put("status", "prepared");
		c_jo.put("pending", steps + "");
		c_jo.put("progress", "0/" + steps);
		c_jo.put("startTime", dateSource);
		c_jo.put("targetTime", dateTarget);
		c_jo.put("intervalSecondsUnit", "1800");
		c_jo.put("intervalSecondsTotal", intervalTime);
		JSONObject m_jo = new JSONObject();
		m_jo.put("status", "waiting");
		m_jo.put("pending", "0");
		m_jo.put("progress", "0/" + steps);
		m_jo.put("startTime", dateSource);
		m_jo.put("targetTime", dateTarget);
		m_jo.put("intervalSecondsUnit", "1800");
		m_jo.put("intervalSecondsTotal", intervalTime);
		JSONObject p_jo = new JSONObject();
		p_jo.put("status", "waiting");
		p_jo.put("pending", "0");
		p_jo.put("progress", "0/" + steps);
		p_jo.put("startTime", dateSource);
		p_jo.put("targetTime", dateTarget);
		p_jo.put("intervalSecondsUnit", "1800");
		p_jo.put("intervalSecondsTotal", intervalTime);
		JSONObject n_jo = new JSONObject();
		n_jo.put("create", c_jo.toString());
		n_jo.put("mix", m_jo.toString());
		n_jo.put("produce", p_jo.toString());
		String nodeStatus = n_jo.toString();
		try {
			client.setData().forPath(nodePath, nodeStatus.getBytes());
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	private static String getRealPath() {
		String rootPath = Playback.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}
}
