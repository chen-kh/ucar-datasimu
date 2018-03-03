package buaa.act.ucar.datasimu.obspat;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.ProcStatus;
import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.core2.DataMixHelper2;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class MixObserver extends Observer {
	private static Logger logger = LogManager.getLogger();
	private String jobId;
	private int nodeId;
	private String[] totalDevList;
	private int multiples;
	private int intervalSeconds_mix;
	private int mixThreadNum;

	public MixObserver(Subject subject) {
		this.subject = subject;
		this.subject.attach(this);
	}

	public void setMixPara(String[] totalDevList, int multiples, int intervalSeconds_mix, int mixThreadNum,
			String jobId, int nodeId) {
		this.totalDevList = totalDevList;
		this.multiples = multiples;
		this.intervalSeconds_mix = intervalSeconds_mix;
		this.mixThreadNum = mixThreadNum;
		this.jobId = jobId;
		this.nodeId = nodeId;
	}

	@Override
	public void update() {
		JSONObject jo = subject.getState();
		JSONObject createStatusJo = JSONObject.fromObject(jo.getString("create"));
		JSONObject mixStatusJo = JSONObject.fromObject(jo.getString("mix"));
		JSONObject produceStatusJo = JSONObject.fromObject(jo.getString("produce"));
		ProcStatus curStatus = ProcStatus.getStatusByExp(mixStatusJo.getString("status"));
		switch (curStatus) {
		case Prepared:
			logger.info("=========================== mix =======================");
			logger.info("nodeId = " + nodeId + ", begin to mix data ...");
			ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Mixing.getExp());
			int step = Integer.parseInt(mixStatusJo.getString("progress").split("/")[0]);
			mixData(totalDevList.length * multiples, totalDevList.length * multiples, intervalSeconds_mix, mixThreadNum,
					jobId, nodeId, step);
			int pending = Integer.parseInt(mixStatusJo.getString("pending"));
			if (pending == 1
					&& ProcStatus.getStatusByExp(createStatusJo.getString("status")).equals(ProcStatus.Waiting)) {
				ZkUtils.updateCreateStatus(jobId, nodeId, ProcStatus.Prepared.getExp());
				logger.info("the current mix_pending is null, change create status from [waiting] to [prepared]");
			}
			ZkUtils.updateMixPending(jobId, nodeId, -1);
			break;
		case Mixed:
			// 更新progress和produce node
			logger.info("------------------------ mix --------------------------");
			String[] stepInfo = mixStatusJo.getString("progress").split("/");
			int nextStep = Integer.parseInt(stepInfo[0]) + 1;
			int steps = Integer.parseInt(stepInfo[1]);
			logger.info("nodeId = " + nodeId + ", mix data task finish " + nextStep + "/" + steps);
			ZkUtils.updateMixProgress(jobId, nodeId, nextStep + "/" + steps);
			ProcStatus prodStatus = ProcStatus.getStatusByExp(produceStatusJo.getString("status"));
			if (prodStatus.equals(ProcStatus.Waiting)) {
				ZkUtils.updateProduceStatus(jobId, nodeId, ProcStatus.Prepared.getExp());
				ZkUtils.updateProducePending(jobId, nodeId, +1);
			} else if (prodStatus.equals(ProcStatus.Producing)) {
				ZkUtils.updateProducePending(jobId, nodeId, +1);
			}
			if (nextStep == steps) {
				ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Done.getExp());
				logger.info(jobId + " - mix stage - done!");
			} else {
				ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Waiting.getExp());
			}
			break;
		case Waiting:
			int pendingWait = Integer.parseInt(mixStatusJo.getString("pending"));
			if (pendingWait > 0) {
				ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Prepared.getExp());
			}
			break;
		default:
			logger.info("------------------------ mix --------------------------");
			logger.info("MixObserver update but do nothing ...");
			logger.info("current mix status: " + mixStatusJo.getString("status"));
			logger.info("the mix node info:\n\t\tmix - " + mixStatusJo.toString());
			break;
		}
		logger.info("^^^^^^^^^^^^^^^^^^^^^ mix observer update done ^^^^^^^^^^^^^^^^^^^^^^^^");
	}

	private void mixData(int carNum4EachOriginalFile, int carNum4EachTargetFile, int intervalSeconds, int threadNum,
			String jobId, int nodeId, int step) {
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
		dataMixHelper.start();
	}
}
