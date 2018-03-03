import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.tools.internal.xjc.api.Mapping;
import com.sun.xml.internal.rngom.nc.NameClassWalker;

import scala.annotation.implicitNotFound;
import scala.reflect.generic.Trees.This;

public class SortedMapConcurrentTest extends Thread {
	private static List<SortedMap<Long, ArrayList<Long>>> myList = new ArrayList<SortedMap<Long, ArrayList<Long>>>();
	private static AtomicInteger finishNum = new AtomicInteger(0);
	static {
		SortedMap<Long, ArrayList<Long>> map = new TreeMap<Long, ArrayList<Long>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
		myList.add(map);
		System.out.println("this is the static code block");
	}

	public void run() {
		for (long i = 0; i < 100 * 10000; i++) {
			synchronized (myList) {
				ArrayList<Long> list = myList.get(0).getOrDefault(i, new ArrayList<Long>());
				// System.out.println(Thread.currentThread().getName() + "\t" +
				// list.size());
				// try {
				// sleep(1000);
				// } catch (InterruptedException e) {
				// // TODO 自动生成的 catch 块
				// e.printStackTrace();
				// }
				list.add(i);
				myList.get(0).put(i, list);
			}
		}
		finishNum.incrementAndGet();
		System.out.println(finishNum.get() + "\t" + System.currentTimeMillis());

	}

	public static void main(String[] args) {
		System.out.println("this is the main code block");
		for (int i = 0; i < 20; i++) {
			new SortedMapConcurrentTest().start();
		}
		for (int i = 0; i < 2; i++) {
			try {
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int size = 0;
			Iterator<Entry<Long, ArrayList<Long>>> iterator = myList.get(0).entrySet().iterator();
			while (iterator.hasNext()) {
				size += iterator.next().getValue().size();
			}
			System.out.println(size);
		}
	}
}
