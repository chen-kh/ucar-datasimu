import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Queue;

import buaa.act.ucar.datasimu.core.DataProHelper;
import buaa.act.ucar.datasimu.kfk.ObdProducer;

public class TestProducer extends Thread {
	public File file;

	public TestProducer(File file) {
		this.file = file;
	}

	public static void main(String[] args) {
		DataProHelper.startAll();
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		File file = new File("E://carsimu/datasimu/test_1000w/mixed/obd_splitor_10s");
		String[] files = file.list();
		for (int i = 0; i < 2; i++) {
			new TestProducer(new File(file.getAbsolutePath() + "/" + files[i])).start();
		}
	}

	public void run() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String message = null;
			long start = System.currentTimeMillis();
			while ((message = reader.readLine()) != null) {
				DataProHelper.addObdMessage2obdProducer(message);
			}
			for (ObdProducer producer : DataProHelper.obdProducers) {
				System.out.println(producer.obdRawQueue.size());
			}
			long end = System.currentTimeMillis();
			System.out.println("time spent = " + (end - start) + "ms");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
