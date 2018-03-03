package buaa.act.ucar.datasimu.core2;

import java.io.File;
import java.util.Arrays;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.file.DataReader;
import buaa.act.ucar.datasimu.obspat.ProduceObserver;
import buaa.act.ucar.datasimu.zk.ZkUtils;

public class DataReadHelper extends Thread {
	private Logger logger = LogManager.getLogger(this.getClass());
	private String jobId;
	private int nodeId;
	private int step;
	private DataReader[] readers = new DataReader[2];

	public DataReadHelper(String jobId, int nodeId, int step) {
		this.jobId = jobId;
		this.nodeId = nodeId;
		this.step = step;
	}

	public void run() {
		String rootPath = CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/mixed/";
		String[] gpsFileNames = new File(rootPath + "/gps").list();
		String[] obdFileNames = new File(rootPath + "/obd").list();
		Arrays.sort(gpsFileNames);
		Arrays.sort(obdFileNames);
		File[] gpsFiles = new File[gpsFileNames.length];
		File[] obdFiles = new File[obdFileNames.length];
		for (int i = 0; i < gpsFiles.length; i++) {
			gpsFiles[i] = new File(rootPath + "/gps/" + gpsFileNames[i]);
			obdFiles[i] = new File(rootPath + "/obd/" + obdFileNames[i]);
		}
		DataReader reader1 = new DataReader("gps", gpsFiles);
		DataReader reader2 = new DataReader("obd", obdFiles);
		reader1.start();
		reader2.start();
		this.readers[0] = reader1;
		this.readers[1] = reader2;
		logger.info("start 2 DataReader theards to read data of " + rootPath);
		sleepUtilReadersDone();
		ProduceObserver.rmDataReadHelper();
		try {
			InterProcessMutex lock = ZkUtils.lockPath("/carsimu/jobs/" + jobId + "/" + nodeId);
			ZkUtils.updateProduceProgress(jobId, nodeId);
			lock.release();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sleepUtilReadersDone() {
		long interval = 1000L;
		while (!(readers[0].isFinish() && readers[1].isFinish())) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
