package buaa.act.ucar.datasimu.obspat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.ProcStatus;
import buaa.act.ucar.datasimu.core2.DataGenHelper2;
import buaa.act.ucar.datasimu.core2.DataGenHelper4large;
import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class CreateObserver extends Observer {
	private static Logger logger = LogManager.getLogger();
	private int method_num;
	private static String jobId;
	private static int nodeId;
	private String[] totalDevList;
	private int carNum4EachGroup;
	private int intervalSeconds4EachCar;
	private int multiples;
	private long delaySeconds;
	private boolean isPlayback=false;

	public CreateObserver(Subject subject) {
		this.subject = subject;
		this.subject.attach(this);
	}

	public void setCreatePara(int method_num, String[] totalDevList, int carNum4EachGroup, int intervalSeconds4EachCar,
			int multiples, long delaySeconds, String jobId, int nodeId) {
		this.method_num = method_num;
		this.totalDevList = totalDevList;
		this.carNum4EachGroup = carNum4EachGroup;
		this.intervalSeconds4EachCar = intervalSeconds4EachCar;
		this.multiples = multiples;
		this.delaySeconds = delaySeconds;
		CreateObserver.jobId = jobId;
		CreateObserver.nodeId = nodeId;
	}

	@Override
	public void update() {
		JSONObject jo = this.subject.getState();
		JSONObject createStatusJo = JSONObject.fromObject(jo.getString("create"));
		JSONObject mixStatusJo = JSONObject.fromObject(jo.getString("mix"));
		ProcStatus curStatus = ProcStatus.getStatusByExp(createStatusJo.getString("status"));
		// good smell
		switch (curStatus) {
		case Prepared:
			logger.info("======================= create ========================");
			logger.info("nodeId = " + nodeId + ", begin to create data file set ...");
			ZkUtils.updateCreateStatus(jobId, nodeId, ProcStatus.Creating.getExp());
			int step = Integer.parseInt(createStatusJo.getString("progress").split("/")[0]);
			// createData()方法中应该在程序执行完之后吧状态更新到created
			createData(method_num, totalDevList, carNum4EachGroup, intervalSeconds4EachCar, multiples, delaySeconds,
					jobId, nodeId, step);
			ZkUtils.updateCreatePending(jobId, nodeId);
			break;
		case Created:
			logger.info("---------------------- create -------------------------");
			String[] stepInfo = createStatusJo.getString("progress").split("/");
			int nextStep = Integer.parseInt(stepInfo[0]) + 1;
			int steps = Integer.parseInt(stepInfo[1]);
			logger.info("nodeId = " + nodeId + ", create data task finish " + nextStep + "/" + steps);
			ZkUtils.updateCreateProgress(jobId, nodeId, nextStep + "/" + steps);
			ProcStatus mixStatus = ProcStatus.getStatusByExp(mixStatusJo.getString("status"));
			switch (mixStatus) {
			case Mixing:
				ZkUtils.updateMixPending(jobId, nodeId, +1);
				break;
			case Waiting:
				ZkUtils.updateMixPending(jobId, nodeId, +1);
				ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Prepared.getExp());
				break;
			default:
				logger.info("create task finish one step and get mix status = " + mixStatus.getExp()
						+ " do nothing with mix");
				break;
			}
			if (nextStep == steps) {
				ZkUtils.updateCreateStatus(jobId, nodeId, ProcStatus.Done.getExp());
				logger.info(jobId + " - create stage - done!");
			} else {
				ZkUtils.updateCreateStatus(jobId, nodeId, ProcStatus.Waiting.getExp());
			}
			break;
		case Done:
			logger.info("---------------------- create -------------------------");
			ProcStatus mixStatus2 = ProcStatus.getStatusByExp(mixStatusJo.getString("status"));
			switch (mixStatus2) {
			case Waiting:
				ZkUtils.updateMixStatus(jobId, nodeId, ProcStatus.Prepared.getExp());
				break;
			default:
				break;
			}
			break;
		default:
			logger.info("---------------------- create -------------------------");
			logger.info("CreateObserver update but do nothing!");
			logger.info("current create status: " + createStatusJo.getString("status"));
			logger.info("the create node info:\n\t\tcreate - " + createStatusJo.toString());
			break;
		}
		logger.info("^^^^^^^^^^^^^^^^^^^^ create observer update done ^^^^^^^^^^^^^^^^^^^^^^^");
	}

	private void createData(int method_num, String[] totalDevList, int carNum4EachGroup, int intervalSeconds4EachCar,
			int multiples, long delaySeconds, String jobId, int nodeId, int step) {
		int group_unit = carNum4EachGroup;
		// 开启所有产生数据线程
		if (method_num == 1) {
			DataGenHelper2 dataGenHelper = new DataGenHelper2(jobId, nodeId, step);
			dataGenHelper.setIsPlayback(isPlayback);
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
			dataGenHelper.setIsPlayback(isPlayback);
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

	public void setIsPlayback() {
		this.isPlayback = true;
	}
}
