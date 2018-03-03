package buaa.act.ucar.datasimu.zk;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;

import buaa.act.ucar.datasimu.config.ZkConfig;

/**
 * zookeeper操作类，用来连接zookeeper,保持会话的类。zk的ip地址保存在Config.ZKADD中。
 * 
 * @author Administrator
 *
 */
public class ZkClient {
	public CuratorFramework client;

	public ZkClient() {
	}

	protected ZkClient(CuratorFramework cli) {
		this.client = cli;
	}

	/**
	 * 建立与zk的连接方法，在阻塞时长内不能连接zk视为连接失败，阻塞时长由配置文件给出
	 * <p>
	 * 阻塞时长为： {@code (Config.RetryTimes + 1) * Config.ConnectTimeOut}
	 * @return
	 */
	public boolean start() {
		String zookeeperConnectionString = ZkConfig.ZKADD;
		RetryPolicy retryPolicy = new RetryNTimes(ZkConfig.RetryTimes, 0);
		client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, ZkConfig.SessionTimeOut,
				ZkConfig.ConnectTimeOut, retryPolicy);
		client.start();
		try {
			int blockTimeoutMs = (ZkConfig.RetryTimes + 1) * ZkConfig.ConnectTimeOut;
			return client.blockUntilConnected(blockTimeoutMs, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		// logger.debug(client.getState());
	}

	/**
	 * 检测给定zk服务ip在阻塞时长内是否可用，可用返回true 阻塞时长默认30s
	 * 
	 * @deprecated
	 * @param zookeeperConnectionString
	 * @return
	 */
	public boolean checkAvailable(String zookeeperConnectionString) {
		RetryPolicy retryPolicy = new RetryNTimes(1, 0);
		client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();
		try {
			if (client.blockUntilConnected(30, TimeUnit.SECONDS)) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			close();
		}
	}

	/**
	 * checkAvailable方法的重载，增加给定阻塞时长参数，单位为秒
	 * 
	 * @param zookeeperConnectionString
	 * @param blockTimeoutS
	 * @return
	 */
	public boolean checkAvailable(String zookeeperConnectionString, int blockTimeoutS) {
		RetryPolicy retryPolicy = new RetryNTimes(1, 0);
		client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();
		try {
			if (client.blockUntilConnected(blockTimeoutS, TimeUnit.SECONDS)) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			close();
		}
	}

	public void close() {
		CloseableUtils.closeQuietly(client);
	}

}
