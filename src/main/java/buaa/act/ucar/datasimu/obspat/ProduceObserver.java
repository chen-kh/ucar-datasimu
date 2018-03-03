package buaa.act.ucar.datasimu.obspat;

import java.io.File;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.ProcStatus;
import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.core.Timer2;
import buaa.act.ucar.datasimu.core2.DataProHelper2;
import buaa.act.ucar.datasimu.core2.DataReadHelper;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class ProduceObserver extends Observer {
	private static Logger logger = LogManager.getLogger();
	private String jobId;
	private int nodeId;
	private int readStepNum;
	private static int dataReadHelperNumber;

	public ProduceObserver(Subject subject) {
		this.subject = subject;
		this.readStepNum = 0;
		ProduceObserver.dataReadHelperNumber = 0;
		this.subject.attach(this);
	}

	public void setProdPara(String jobId, int nodeId) {
		this.jobId = jobId;
		this.nodeId = nodeId;
	}

	@Override
	public void update() {
		JSONObject jo = subject.getState();
		System.out.println(jo);
		JSONObject prodStatusJo = JSONObject.fromObject(jo.getString("produce"));
		ProcStatus curStatus = ProcStatus.getStatusByExp(prodStatusJo.getString("status"));
		// good smell
		switch (curStatus) {
		case Prepared:
			logger.info("======================= produce ========================");
			logger.info("nodeId = " + nodeId + ", begin to produce data file set ...");
			ZkUtils.updateProduceStatus(jobId, nodeId, ProcStatus.Producing.getExp());
			int step = Integer.parseInt(prodStatusJo.getString("progress").split("/")[0]);
			// in fact step must be 0, and prepared status only presents 1 time
			startAllProducer(jobId, nodeId, step);
			readData(jobId, nodeId, step);
			break;
		case Producing:
			logger.info("---------------------- produce -------------------------");
			int pending = Integer.parseInt(prodStatusJo.getString("pending").split("/")[0]);
			if (pending > 0 && dataReadHelperNumber < 2) {
				readData(jobId, nodeId, this.readStepNum);
				logger.info("observed producing but with pending = " + pending + ", add 2 threads to read data, step = "
						+ (this.readStepNum - 1));
			} else {
				logger.info("ProduceObserver - current status is producting but there is no pending!");
				logger.info("current produce status: " + prodStatusJo.getString("status"));
				logger.info("the produce node info:\n\t\tproduce - " + prodStatusJo.toString());
			}
			break;
		default:
			logger.info("---------------------- produce -------------------------");
			logger.info("ProduceObserver update but do nothing!");
			logger.info("current produce status: " + prodStatusJo.getString("status"));
			logger.info("the produce node info:\n\t\tproduce - " + prodStatusJo.toString());
			break;
		}
		logger.info("^^^^^^^^^^^^^^^^^^^ produce observer update done ^^^^^^^^^^^^^^^^^^^^^^");
	}

	public static void addDataReadHelper() {
		ProduceObserver.dataReadHelperNumber++;
		logger.info(
				"add one dataReadHelper, the current dataReaderHelperNumber = " + ProduceObserver.dataReadHelperNumber);
	}

	public static void rmDataReadHelper() {
		ProduceObserver.dataReadHelperNumber--;
		logger.info(
				"remove one dataReadHelper(done), the current dataReaderHelperNumber = " + ProduceObserver.dataReadHelperNumber);
	}

	private void startAllProducer(String jobId, int nodeId, int step) {
		String rootPath = CommonConfig.getRealPath() + "/" + jobId + "/" + nodeId + "/" + step + "/mixed/";
		String[] fileNames = new File(rootPath + "/gps").list();
		Arrays.sort(fileNames);
		long initTimeId = Long.parseLong(fileNames[0].split("_")[0]) - 10L;
		Timer2.instance.setInitTimeId(initTimeId);
		Timer2.instance.start();
		DataProHelper2.setThreadNum(8);
		DataProHelper2.initProducers();
		DataProHelper2.startAll();
		logger.info("ProduceObserver: start all producers done!");
	}

	private void readData(String jobId, int nodeId, int step) {
		logger.info("start data read helper for jobId = " + jobId + ", nodeId = " + nodeId + ", step = " + step);
		this.readStepNum += 1;
		new DataReadHelper(jobId, nodeId, step).start();
		ZkUtils.updateProducePending(jobId, nodeId, -1);
		addDataReadHelper();
	}
}
