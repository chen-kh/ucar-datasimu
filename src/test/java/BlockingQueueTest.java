import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.xml.internal.rngom.nc.NameClassWalker;

public class BlockingQueueTest extends Thread {
	private static Map<Integer, LinkedBlockingQueue<Integer>> queueMap = new HashMap<Integer, LinkedBlockingQueue<Integer>>();
	private static LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();
	private static List<LinkedBlockingQueue<Integer>> list = new ArrayList<LinkedBlockingQueue<Integer>>();
	private int num = 0;

	public BlockingQueueTest(int num) {
		this.num = num;
	}

	static {
		// LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
		for (int j = 0; j < 1; j++) {
			for (int i = 0; i < 1000 * 10000; i++) {
				try {
					queue.put(i);
				} catch (Exception e) {
					e.printStackTrace();
				}
				list.add(queue);
				// queueMap.put(j, queue);
			}
		}
	}

	public void run() {
		for (int i = 0; i < 100 * 1000; i++) {
			try {
				// queueMap.get(num).take();
				// queue.take();
				list.get(num).take();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < 20; i++) {
			new BlockingQueueTest(0).start();
		}
		for (int i = 0; i < 2; i++) {
			try {
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// System.out.println("i = " + i + "\t" +
			// BlockingQueueTest.queueMap.get(0).size());
			System.out.println("i = " + i + "\t" + BlockingQueueTest.list.get(0).size());
		}
//		try {
//			System.out.println("for queueMap : if safe num =  " + (1000 * 1000 - 20 * 10 * 1000) + "\nreal num = "
//					+ BlockingQueueTest.queueMap.get(0).size() + "\tpresent num = "
//					+ BlockingQueueTest.queueMap.get(0).take());
//			System.out.println("for queue : if safe num =  " + (1000 * 1000 - 20 * 10 * 1000) + "\nreal num = "
//					+ BlockingQueueTest.queue.size() + "\tpresent num = " + BlockingQueueTest.queue.take());
//		} catch (Exception e) {
//			// TODO 自动生成的 catch 块
//			e.printStackTrace();
//		}

	}
}
