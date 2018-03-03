package buaa.act.ucar.datasimu.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.core.Timer2;
import buaa.act.ucar.datasimu.core2.DataProHelper2;

public class DataReader extends Thread {
	private Logger logger = LogManager.getLogger(this.getClass());
	private String name;
	private File[] files;
	private boolean isFinish;
	private String jobNodeStepPath;

	public DataReader(String name, File[] files) {
		this.name = name;
		this.files = files;
		this.isFinish = false;
		this.jobNodeStepPath = files[0].getParent();
	}

	public void run() {
		long timeId = 0;
		if (name.contains("gps")) {
			for (int i = 0; i < files.length; i++) {
				timeId = Long.parseLong(files[i].getName().split("_")[0]);
				logger.info("read file : " + files[i]);
				sleepUntilAllow2readNextFile(timeId);
				File file = files[i];
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(file));
					String message = null;
					long start = System.currentTimeMillis();
					while ((message = reader.readLine()) != null) {
						DataProHelper2.addGpsMessage2gpsProducer(message);
					}
					long end = System.currentTimeMillis();
					logger.info("read one gps file(in 10s) time spent = " + (end - start) + "ms");
				} catch (IOException e) {
					logger.error(e);
					e.printStackTrace();
				}finally {
					try {
						reader.close();
					} catch (IOException e) {
						logger.error(e);
						e.printStackTrace();
					}	
				}
				
			}
			deleteAllGpsFiles(jobNodeStepPath);
		}
		if (name.contains("obd")) {
			for (int i = 0; i < files.length; i++) {
				timeId = Long.parseLong(files[i].getName().split("_")[0]);
				logger.info("read file : " + files[i]);
				sleepUntilAllow2readNextFile(timeId);
				File file = files[i];
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(file));
					String message = null;
					long start = System.currentTimeMillis();
					while ((message = reader.readLine()) != null) {
						DataProHelper2.addObdMessage2obdProducer(message);
					}
					long end = System.currentTimeMillis();
					logger.info("read one obd file(in 10s) time spent = " + (end - start) + "ms");
				} catch (IOException e) {
					e.printStackTrace();
				}finally {
					try {
						reader.close();
					} catch (IOException e) {
						logger.error(e);
						e.printStackTrace();
					}
				}
			}
			deleteAllObdFiles(jobNodeStepPath);
		}
		this.isFinish = true;
	}
	public boolean isFinish(){
		return isFinish;
	}
	private void deleteAllGpsFiles(String path) {
		String[] files = new File(path).list();
		logger.info("begin to delete all gps files in " + path);
		logger.info("total gps file number = " + files.length);
		for (String file : files) {
			new File(path + "/" + file).delete();
		}
		logger.info("delete all gps files done !");
	}

	private void deleteAllObdFiles(String path) {
		String[] files = new File(path).list();
		logger.info("begin to delete all obd files in " + path);
		logger.info("total obd files number = " + files.length);
		for (String file : files) {
			new File(path + "/" + file).delete();
		}
		logger.info("delete all obd files done !");

	}

	private void sleepUntilAllow2readNextFile(long timeId) {
		Timer2 timer = Timer2.instance;
		final long intervalSeconds = 500;
		while (timeId > timer.getCurrentTimeId()) {
			try {
				Thread.sleep(intervalSeconds);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
