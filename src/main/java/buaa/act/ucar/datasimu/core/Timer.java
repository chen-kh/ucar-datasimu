package buaa.act.ucar.datasimu.core;

/**
 * 计时器，每十秒钟给timeId增加1
 * 
 * @author 00000000000000000000
 *
 */
public class Timer extends Thread {
	private static long timeId = -3L;
	// not lazy loading
	public static Timer instance = new Timer();

	private Timer() {

	}

	public void run() {
		while (true) {
			System.out.println("-------------------- timeId = " + timeId + " ------------------------");
			try {
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			timeId++;
		}
	}

	public long getCurrentTimeId() {
		return timeId;
	}
}
