package buaa.act.ucar.datasimu.core2;

import java.util.ArrayList;
import java.util.List;

import buaa.act.ucar.datasimu.config.CommonConfig;
import buaa.act.ucar.datasimu.kfk.GpsProducer;
import buaa.act.ucar.datasimu.kfk.ObdProducer;

public class DataProHelper2 {
	private static int producerNum = 8;
	private static List<GpsProducer> gpsProducers = new ArrayList<GpsProducer>(producerNum);
	public static List<ObdProducer> obdProducers = new ArrayList<ObdProducer>(producerNum);

	public static void initProducers() {
		for (int i = 0; i < producerNum; i++) {
			GpsProducer gpsProducer = new GpsProducer(CommonConfig.GpsTopicId);
			gpsProducers.add(gpsProducer);
			ObdProducer obdProducer = new ObdProducer(CommonConfig.ObdTopicId);
			obdProducers.add(obdProducer);
		}
	}

	public static void startAll() {
		for (GpsProducer gpsProducer : gpsProducers) {
			gpsProducer.start();
		}
		for (ObdProducer obdProducer : obdProducers) {
			obdProducer.start();
		}
	}

	public static void addGpsMessage2gpsProducer(String msg) {
		gpsProducers.get(Math.abs(msg.hashCode()) % producerNum).tryAddGpsMessage(msg);
	}

	public static void addObdMessage2obdProducer(String msg) {
		obdProducers.get(Math.abs(msg.hashCode()) % producerNum).tryAddObdMessage(msg);
	}

	public static void setThreadNum(int threadNum) {
		DataProHelper2.producerNum = threadNum;
	}
}
