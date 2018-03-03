package buaa.act.ucar.datasimu.manager;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.Simulator;
import buaa.act.ucar.datasimu.core.DataProHelper;
import buaa.act.ucar.datasimu.core.Timer2;
import buaa.act.ucar.datasimu.file.DataReader;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class ProduceManager extends Thread {
	private Logger logger = LogManager.getLogger();
	private int id;
	public ProduceManager(int id) {
		this.id = id;
	}

	public void run() {
		while (!allow2produceData()) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		updateProduceStatus(id, 0);
		logger.info("begin to produce data ...");
		produceData();
		updateProduceStatus(id, 1);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void updateProduceStatus(int id, int status) {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		jo.put("produced", status);
		ZkUtils.setData(path, jo.toString());
		logger.info("zk client updated [produced_status] for " + path + " to [" + status + "]");
	}

	private boolean allow2produceData() {
		String path = "/carsimu/devlist/status/" + id;
		JSONObject jo = JSONObject.fromObject(ZkUtils.getData(path));
		int producedStatus = jo.getInt("produced");
		if (producedStatus == 2)
			return true;
		return false;
	}

	/**
	 * 数据生产到kafka，保证每条数据的时间戳和发送时间的差值不超过10s
	 */
	private void produceData() {
		String rootPath = getRealPath() + "/mixed2/";
		long initTimeId = Long.parseLong(new File(rootPath + "gps").list()[0].split("_")[0]) - 20L;
		Timer2.instance.setInitTimeId(initTimeId);
		Timer2.instance.start();
		DataProHelper.setThreadNum(8);
		DataProHelper.initProducers();
		DataProHelper.startAll();
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

	private static String getRealPath() {
		String rootPath = Simulator.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		return rootPath;
	}
}
