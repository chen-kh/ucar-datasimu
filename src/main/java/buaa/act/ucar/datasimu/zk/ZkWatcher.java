package buaa.act.ucar.datasimu.zk;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import buaa.act.ucar.datasimu.obspat.Subject;
import net.sf.json.JSONObject;

public class ZkWatcher extends Thread {
	private Logger logger = LogManager.getLogger(this.getClass());
	private CuratorFramework client;
	private int nodeId;
	private String jobId;
	private Subject subject;

	public ZkWatcher(Subject subject, String jobId, int nodeId) {
		this.subject = subject;
		ZkClient client = new ZkClient();
		client.start();
		this.client = client.client;
		this.jobId = jobId;
		this.nodeId = nodeId;
	}

	public void run() {
		try {
			beginWatch(client, jobId, nodeId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setId(int id) {
		this.nodeId = id;
	}

	private void beginWatch(CuratorFramework client, String jobId, int nodeId) throws Exception {
		String nodePath = "/carsimu/jobs/" + jobId + "/" + nodeId;
		final NodeCache nodeCache_node = new NodeCache(client, nodePath);
		NodeCacheListener nodeListener = new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				logger.info("----------- zookeeper watcher get node changed ----------");
				logger.info("path : " + nodeCache_node.getCurrentData().getPath());
				String data = new String(nodeCache_node.getCurrentData().getData());
				logger.info("data : " + data);
//				logger.info("stat : " + nodeCache_node.getCurrentData().getStat());
				JSONObject state = JSONObject.fromObject(data);
				subject.setState(state);
			}
		};
		nodeCache_node.getListenable().addListener(nodeListener);
		nodeCache_node.start();
		Thread.sleep(Long.MAX_VALUE);
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void setListenterPersRec(String path) throws Exception {
		PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				logger.info("--------- get an event, start to analyse -----");
				ChildData data = event.getData();
				switch (event.getType()) {
				case CHILD_ADDED:
					logger.info("CHILD_ADDED : " + data.getPath() + "  value:" + new String(data.getData()));
					break;
				case CHILD_REMOVED:
					logger.info("CHILD_REMOVED : " + data.getPath() + "  value:" + new String(data.getData()));
					break;
				case CHILD_UPDATED:
					logger.info("CHILD_UPDATED : " + data.getPath() + "  value:" + new String(data.getData()));
					break;
				default:
					break;
				}
			}
		};
		ExecutorService pool = Executors.newCachedThreadPool();
		PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
		childrenCache.getListenable().addListener(childrenCacheListener);
		logger.info("Register zk watcher to " + path + " successfully!");
		childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
		Thread.sleep(Long.MAX_VALUE);
	}

	private void setListenterOne(final CuratorFramework client) throws Exception {
		// 注册观察者，当节点变动时触发
		byte[] data = null;
		try {
			data = client.getData().usingWatcher(new Watcher() {
				public void process(WatchedEvent event) {
					logger.info("获取 watcher 节点 监听器 : " + event);
					try {
						logger.info(new String(client.getData().forPath(event.getPath())));
					} catch (Exception e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
				}
			}).forPath("/test/watcher");
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("watcher 节点数据: " + new String(data));
		Thread.sleep(100000);
	}

	private void setListenterTwo(CuratorFramework client) throws Exception {
		ExecutorService pool = Executors.newCachedThreadPool();
		CuratorListener listener = new CuratorListener() {
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				logger.info("监听器  : " + event.toString());
			}

		};
		client.getCuratorListenable().addListener(listener, pool);
		client.getData().inBackground().forPath("/test/watcher");
		client.getData().inBackground().forPath("/test/watcher");
		client.getData().inBackground().forPath("/test/watcher");
		client.getData().inBackground().forPath("/test/watcher");
		Thread.sleep(Long.MAX_VALUE);
	}

	private void setListenterThreeTwo(CuratorFramework client, int id) throws Exception {
		ExecutorService pool = Executors.newCachedThreadPool();
		// 设置节点的cache
		final NodeCache nodeCache = new NodeCache(client, "/carsimu/devlist/status/" + id, false);
		NodeCacheListener listener = new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				String data = new String(nodeCache.getCurrentData().getData());
				logger.info("Node is changed and result is : " + "\n\tpath = " + nodeCache.getCurrentData().getPath()
						+ "\n\tdata = " + data);
				JSONObject jo = JSONObject.fromObject(data);
				if (jo.getInt("produced") == 2) {
					// which means that data has been prepared
				}
			}
		};
		nodeCache.getListenable().addListener(listener);
		nodeCache.start();
		Thread.sleep(Long.MAX_VALUE);
	}

	// 3.Tree Cache
	// 监控 指定节点和节点下的所有的节点的变化--无限监听 可以进行本节点的删除(不在创建)
	private void setListenterThreeThree(CuratorFramework client) throws Exception {
		ExecutorService pool = Executors.newCachedThreadPool();
		// 设置节点的cache
		TreeCache treeCache = new TreeCache(client, "/test");
		// 设置监听器和处理过程
		TreeCacheListener listener = new TreeCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				System.out.println("get an event");
				ChildData data = event.getData();
				if (data != null) {
					switch (event.getType()) {
					case NODE_ADDED:
						System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;
					case NODE_REMOVED:
						System.out.println("NODE_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;
					case NODE_UPDATED:
						System.out.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;

					default:
						break;
					}
				} else {
					System.out.println("data is null : " + event.getType());
				}
			}
		};
		treeCache.getListenable().addListener(listener);
		// 开始监听
		treeCache.start();
		Thread.sleep(Long.MAX_VALUE);
	}
}
