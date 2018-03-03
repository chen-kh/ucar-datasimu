package buaa.act.ucar.datasimu.hbase;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class Utils {
	/*
	 * rowkey的设计 反转的devicesn后六位+devicesn+ long最大值减去当前时间戳
	 */
	
    public static String getDecPrefix(String devicesn){
        StringBuffer sb = new StringBuffer(devicesn.substring(6,devicesn.length()));
        return sb.reverse().toString();
    }
    
	
	public static byte[] generateRowkeyPM(long time, String devicesn) {
		return Bytes.toBytes(Utils.getDecPrefix(devicesn) + devicesn
				+ Long.toString(Long.MAX_VALUE - time));
	}
	public static long getTimeStamp(String dateString) {
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		ts = Timestamp.valueOf(dateString);
		// format timestamp from milliseonds to seconds
		return ts.getTime() / 1000;

	}

	public static String getDate(long timestamp) {
		Date date = new Date(timestamp * 1000);
		return date.toString();
	}
}
