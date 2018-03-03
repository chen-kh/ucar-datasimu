import buaa.act.ucar.datasimu.config.CommonConfig;

public class Test {
	public static void main(String[] args) throws Exception {
		String rootPath = CommonConfig.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		rootPath = rootPath.substring(0, rootPath.lastIndexOf("/") + 1);
		System.out.println(rootPath);
	}
}
