package buaa.act.ucar.datasimu.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class FileHelper {
	private static final int threadNum = 25;
	private static List<GpsReader> gpsReaders = new ArrayList<GpsReader>(threadNum);
	private static List<ObdReader> obdReaders = new ArrayList<ObdReader>(threadNum);

	static {
		for (int i = 0; i < threadNum; i++) {
			GpsReader gpsReader = new GpsReader();
			ObdReader obdReader = new ObdReader();
			gpsReaders.add(gpsReader);
			obdReaders.add(obdReader);
			// gpsReader.start();
			obdReader.start();
		}
	}

	public static void distributeFileSetStepByStep(String rootPath) {
		File parentFile = new File(rootPath);
		String[] files = parentFile.list();
		SortedMap<Long, List<File>> map = new TreeMap<Long, List<File>>(new Comparator<Long>() {
			public int compare(Long arg0, Long arg1) {
				return (int) (arg0 - arg1);
			}
		});
		for (String fileName : files) {
			// int timeId = Integer.parseInt(fileName.split("_")[0]);
			long timeId = Long.parseLong(fileName.split("_")[0]);
			File file = new File(rootPath + "/" + fileName);
			List<File> list = map.getOrDefault(timeId, new ArrayList<File>());
			list.add(file);
			map.put(timeId, list);
		}
		Iterator<Entry<Long, List<File>>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<Long, List<File>> entry = iterator.next();
			// distributeGpsFileSet(entry.getValue());
			distributeObdFileSet(entry.getValue());
		}
	}

	private static void distributeGpsFileSet(List<File> list) {
		if (threadNum == list.size()) {
			for (int i = 0; i < threadNum; i++) {
				gpsReaders.get(i).tryAddFile(list.get(i));
			}
		}
	}

	private static void distributeObdFileSet(List<File> list) {
		if (threadNum == list.size()) {
			for (int i = 0; i < threadNum; i++) {
				obdReaders.get(i).tryAddFile(list.get(i));
			}
		}
	}

}
