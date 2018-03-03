package buaa.act.ucar.datasimu;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.hbase.Utils;
import buaa.act.ucar.datasimu.manager.CreateManager;
import buaa.act.ucar.datasimu.manager.MixManager;
import buaa.act.ucar.datasimu.manager.ProduceManager;
import buaa.act.ucar.datasimu.zk.ZkClient;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

/**
 * 总体思想分为三步，创建数据->混合数据->生产数据
 * 
 * @deprecated 原因是Simulator2比这个更简洁，占内存更小，速度也更快。我当时真实脑子gg了才想出这么复杂的方法
 * @author 00000000000000000000
 *
 */
public class Simulator {
	private Logger logger = LogManager.getLogger();

	public static void main(String[] args) {
		if (args != null && args.length == 4) {
			long dateSource = Utils.getTimeStamp(args[0]);
			long dateTarget = Utils.getTimeStamp(args[1]);
			long intervalSeconds = Long.parseLong(args[2]);// 必须是1800的倍数
			int carsimuNumber = Integer.parseInt(args[3]);// 必须是25000的整数倍
			ZkClient zkClient = new ZkClient();
			zkClient.start();
			CuratorFramework opter = zkClient.client;
			initZkStatus(dateSource, dateTarget);
			String devDataPath = "/carsimu/devlist/data";
			String numPath = "/carsimu/devlist/status/num";
			int id = -1;
			String[] totalDevList = null;
			// 获取一个大集合里面的车辆集合
			try {
				InterProcessMutex lock = new InterProcessMutex(opter, numPath);
				while (!lock.acquire(10, TimeUnit.SECONDS)) {
					Thread.sleep(1000);
					System.out.println(Thread.currentThread().getName() + " is waiting for lock of " + numPath);
				}
				int num = Math.abs(Integer.parseInt(new String(opter.getData().forPath(numPath))));
				id = num % 5;
				opter.setData().forPath(numPath, (--num + "").getBytes());
				lock.release();
				totalDevList = new String(opter.getData().forPath(devDataPath + "/" + id)).split("\\|");
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("something wrong when get id of big_group");
				if (id == -1) {
					System.exit(0);
				}
			}
			zkClient.close();
			int multiples = (int) (carsimuNumber / 25000);
			CreateManager createManager = new CreateManager(id, multiples);
			createManager.setTotalDevList(totalDevList);
			createManager.setTotalIntervalSeconds(intervalSeconds);
			createManager.start();
			MixManager mixManager = new MixManager(id, multiples);
			mixManager.setRunTimes((int) (intervalSeconds / 1800));
			mixManager.start();
			ProduceManager produceManager = new ProduceManager(id);
			produceManager.start();
		} else {
			System.out.println("Usage: <dateSource> <dateTarget> <intervalSeconds> <carsimuNumber>");
		}

	}

	private static void initZkStatus(long dateSource, long dateTarget) {
		String path = "/carsimu/devlist/status";
		for (int i = 0; i < 5; i++) {
			JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path + "/" + i));
			jo.put("startTime", dateSource);
			jo.put("targetTime", dateTarget);
			jo.put("created", -1);
			jo.put("mixed", -1);
			jo.put("produced", -1);
			ZkUtils.setData(path + "/" + i, jo.toString());
		}
	}
}