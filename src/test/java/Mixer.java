import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import scala.xml.Atom;

public class Mixer extends Thread {
	private static AtomicInteger integer = new AtomicInteger(0);

	public static void main(String[] args) {
		for (int i = 0; i < 20; i++) {
			new Mixer().start();
		}
		for (int i = 0; i < 20; i++) {
			integer.incrementAndGet();
		}
	}

	public void run() {
		while (integer.get() != 20) {
//			try {
//				Thread.sleep(1);
//			} catch (InterruptedException e) {
//				// TODO 自动生成的 catch 块
//				e.printStackTrace();
//			}
		}
		System.out.println(Thread.currentThread().getName() + "\t" + integer.get());
	}
}
