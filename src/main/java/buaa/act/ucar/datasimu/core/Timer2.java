package buaa.act.ucar.datasimu.core;

public class Timer2 extends Thread {
	private static long timeId = -30L;
	// not lazy loading
	public static Timer2 instance = new Timer2();

	private Timer2() {

	}

	public void run() {
		while (true) {
			timeId += 10;
			System.out.println("-------------------- timeId = " + timeId + " ------------------------");
			try {
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public long getCurrentTimeId() {
		return timeId;
	}

	public void setInitTimeId(long timeId) {
		Timer2.timeId = timeId;
	}
}