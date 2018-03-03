package buaa.act.ucar.datasimu;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum ProcStatus {
	// create status
	Waiting("waiting"), Prepared("prepared"), Done("done"), Creating("creating"), Created("created"), Mixing(
			"mixing"), Mixed("mixed"), Producing("producing"), Produced("produced");
	// mix status
	// produce status
	private static Logger logger = LogManager.getLogger();
	private String exp;

	private ProcStatus(String exp) {
		this.exp = exp;
	}

	public String getExp() {
		return exp;
	}

	public static ProcStatus getStatusByExp(String exp) {
		switch (exp) {
		case "waiting":
			return Waiting;
		case "prepared":
			return Prepared;
		case "done":
			return Done;
		case "created":
			return Created;
		case "creating":
			return Creating;
		case "mixing":
			return Mixing;
		case "mixed":
			return Mixed;
		case "producing":
			return Producing;
		case "produced":
			return Produced;
		default:
			logger.error("ERROR: get wrong status! status = " + exp);
			System.exit(0);
		}
		return null;
	}

	public static void main(String[] args) {
		ProcStatus status = getStatusByExp("");
		System.out.println(status.equals(Creating));
	}
}
