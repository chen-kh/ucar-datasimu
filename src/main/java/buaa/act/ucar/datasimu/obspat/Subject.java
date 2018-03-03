package buaa.act.ucar.datasimu.obspat;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import buaa.act.ucar.datasimu.zk.ZkUtils;
import net.sf.json.JSONObject;

public class Subject {
	private static Logger logger = LogManager.getLogger();
	private List<Observer> observers = new ArrayList<Observer>();
	// private int state;
	private JSONObject state;
	private String jobId;
	private int nodeId;

	public JSONObject getState() {
		return state;
	}

	public void setState(JSONObject state) {
		this.state = state;
		notifyAllObservers();
	}

	public void attach(Observer observer) {
		observers.add(observer);
	}

	public void remove(Observer observer) {
		observers.remove(observer);
	}

	public void notifyAllObservers() {
		InterProcessMutex lock;
		try {
			lock = ZkUtils.lockPath("/carsimu/jobs/" + jobId + "/" + nodeId);
			for (Observer observer : observers) {
				try {
					observer.update();
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
					logger.error("ERROR: got error while observer update. Prepare to exit");
					System.exit(1);
				}
			}
			lock.release();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<Observer> getObservers() {
		return this.observers;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
}
