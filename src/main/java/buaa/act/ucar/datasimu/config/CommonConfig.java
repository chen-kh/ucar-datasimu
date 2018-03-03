package buaa.act.ucar.datasimu.config;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class CommonConfig {
	
//	private static String DB;
	private static String realPath = getRootPath();
	public static String HBaseQuorum;
	public static String HBaseTimeout;
	public static String HBaseCaching;
	public static String BROKER_LIST;
	public static String ZKADD;
	public static int RetryTimes;
	public static int SessionTimeOut;
	public static int ConnectTimeOut;
	public static int TIMEOUT;
	public static int INTERVAL;
	public static String GROUP_ID;
	public static boolean KfkProdAsync;
	public static String GpsTopicId;
	public static String ObdTopicId;
	private static String getRootPath() {
		String rootPath = CommonConfig.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		System.out.println(rootPath);
//		rootPath = "E:/workspace_trustcars/ucar-datasimu/src/main/resources/";
		return rootPath;
	}
	public static String getRealPath(){
		return realPath;
	}
	// 从配置文件读取上述未初始化的参数
	static {
		Properties props = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(getRealPath() + "//datasimu.properties"));
			props.load(in);
			// Hbase config
			CommonConfig.HBaseQuorum = props.getProperty("HBaseQuorum");
			CommonConfig.HBaseTimeout = props.getProperty("HBaseTimeout");
			CommonConfig.HBaseCaching = props.getProperty("HBaseCaching");
			// Kfk config
			CommonConfig.KfkProdAsync = Boolean.parseBoolean(props.getProperty("KfkProduceAsync"));
			CommonConfig.BROKER_LIST = props.getProperty("BROKER_LIST");			
			CommonConfig.GROUP_ID = props.getProperty("GROUP_ID");
			CommonConfig.INTERVAL = Integer.parseInt(props.getProperty("INTERVAL"));
			CommonConfig.TIMEOUT = Integer.parseInt(props.getProperty("TIMEOUT"));
			// Zk config
			CommonConfig.ZKADD = props.getProperty("ZKADD");
			CommonConfig.RetryTimes = Integer.parseInt(props.getProperty("RetryTimes"));
			CommonConfig.SessionTimeOut = Integer.parseInt(props.getProperty("SessionTimeOut"));
			CommonConfig.ConnectTimeOut = Integer.parseInt(props.getProperty("ConnectTimeOut"));
			// producer config
			CommonConfig.GpsTopicId = props.getProperty("GpsTopicId");
			CommonConfig.ObdTopicId = props.getProperty("ObdTopicId");
			in.close();
			System.out.println(Thread.currentThread().getName() + " load CommonConfig file: " + props.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
