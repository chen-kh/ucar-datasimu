package buaa.act.ucar.datasimu.zk;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.json.JSONObject;
import scala.reflect.generic.Trees.If;

public class ZkUtils {
	private static Logger logger = LogManager.getLogger();
	public static ZkClient zkClient = new ZkClient();

	static {
		zkClient.start();
	}

	public static String getData(String path) {
		CuratorFramework client = zkClient.client;
		try {
			String res = new String(client.getData().forPath(path));
			return res;
		} catch (Exception e) {
			logger.info(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public static void setData(String path, String value) {
		CuratorFramework client = zkClient.client;
		try {
			client.setData().forPath(path, value.getBytes());
		} catch (Exception e) {
			logger.info(e.getMessage());
			e.printStackTrace();
		}
	}

	public static void updateCreateStatus(String jobId, int nodeId, String status) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject c_jo = JSONObject.fromObject(jo.getString("create"));
		String preStatus = c_jo.getString("status");
		c_jo.put("status", status);
		jo.put("create", c_jo.toString());
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [create_status] from [" + preStatus + "] to [" + status + "]");
	}

	public static void updateCreateProgress(String jobId, int nodeId, String progress) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject c_jo = JSONObject.fromObject(jo.getString("create"));
		String preProgress = c_jo.getString("progress");
		c_jo.put("progress", progress);
		jo.put("create", c_jo);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [create_progress] from [" + preProgress + "] to [" + progress + "]");
	}

	public static void updateCreatePending(String jobId, int nodeId) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject c_jo = JSONObject.fromObject(jo.getString("create"));
		String prePending = c_jo.getString("pending");
		c_jo.put("pending", (Integer.parseInt(prePending) - 1) + "");
		jo.put("create", c_jo);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [create_pending] from [" + prePending + "] to ["
				+ (Integer.parseInt(prePending) - 1) + "]");
	}

	public static void updateTimeAndInitStatus(int id) {
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

	public static void updateMixStatus(String jobId, int nodeId, String status) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject m_jo = JSONObject.fromObject(jo.getString("mix"));
		String preStatus = m_jo.getString("status");
		m_jo.put("status", status);
		jo.put("mix", m_jo.toString());
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [mix_status] from [" + preStatus + "] to [" + status + "]");
	}

	public static void updateMixProgress(String jobId, int nodeId, String progress) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject m_jo = JSONObject.fromObject(jo.getString("mix"));
		String preProgress = m_jo.getString("progress");
		System.out.println(preProgress);
		m_jo.put("progress", progress);
		jo.put("mix", m_jo.toString());
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [mix_progress] from [" + preProgress + "] to [" + progress + "]");
	}

	public static void updateMixPending(String jobId, int nodeId, int up) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject m_jo = JSONObject.fromObject(jo.getString("mix"));
		String prePending = m_jo.getString("pending");
		m_jo.put("pending", (Integer.parseInt(prePending) + up) + "");
		if (Integer.parseInt(prePending) + up < 0) {
			logger.error("ERROR: mix pending has been setted to " + (Integer.parseInt(prePending) + up));
		}
		jo.put("mix", m_jo);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [mix_pending] from [" + prePending + "] to ["
				+ (Integer.parseInt(prePending) + up) + "]");

	}

	public static void updateProduceStatus(String jobId, int nodeId, String status) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject p_jo = JSONObject.fromObject(jo.getString("produce"));
		String preStatus = p_jo.getString("status");
		p_jo.put("status", status);
		jo.put("produce", p_jo.toString());
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [produce_status] from [" + preStatus + "] to [" + status + "]");
	}

	public static void updateProducePending(String jobId, int nodeId, int up) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		System.out.println(jo);
		JSONObject p_jo = JSONObject.fromObject(jo.getString("produce"));
		String prePending = p_jo.getString("pending");
		p_jo.put("pending", (Integer.parseInt(prePending) + up) + "");
		if (Integer.parseInt(prePending) + up < 0) {
			logger.error("ERROR: produce pending has been setted to " + (Integer.parseInt(prePending) + up));
		}
		jo.put("produce", p_jo);
		System.out.println(jo);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [produce_pending] from [" + prePending + "] to ["
				+ (Integer.parseInt(prePending) + up) + "]");
	}

	public static void updateProduceProgress(String jobId, int nodeId) {
		String path = "/carsimu/jobs/" + jobId + "/" + nodeId;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		JSONObject p_jo = JSONObject.fromObject(jo.getString("produce"));
		String preProgress = p_jo.getString("progress");
		String progress = (Integer.parseInt(preProgress.split("/")[0]) + 1) + "/" + preProgress.split("/")[1];
		p_jo.put("progress", progress);
		jo.put("produce", p_jo.toString());
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [produce_progress] from [" + preProgress + "] to [" + progress + "]");
	}
	public static InterProcessMutex lockPath(String path) throws InterruptedException, Exception{
		InterProcessMutex lock = new InterProcessMutex(zkClient.client, path);
		while (!lock.acquire(10, TimeUnit.SECONDS)) {
			Thread.sleep(1000);
			System.out.println(Thread.currentThread().getName() + " is waiting for lock of " + path);
		}
		return lock;
	}
	public static void deleteAllInfos4jobId(String jobId) {
		CuratorFramework client = zkClient.client;
		try {
			if (client.checkExists().forPath("/carsimu/jobs/" + jobId) != null) {
				client.delete().deletingChildrenIfNeeded().forPath("/carsimu/jobs/" + jobId);
			}
			String nodeIndo = ZkUtils.getData("/carsimu/jobs/nodeInfo");
			JSONObject jo = JSONObject.fromObject(nodeIndo);
			jo.remove(jobId);
			ZkUtils.setData("/carsimu/jobs/nodeInfo", jo.toString());
			System.out.println("DELETE SUCCESS");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("ERROR");
		}
		
	}
}
