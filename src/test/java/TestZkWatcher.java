import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.text.html.parser.TagElement;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import buaa.act.ucar.datasimu.zk.ZkUtils;

public class TestZkWatcher {
	// 测试zookeeper的watcher机制
	public static void main(String[] args) throws Exception {
		System.out.println(Long.MAX_VALUE / 1000d / 60d / 60 / 24d);
		final Object object = new Object();
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					synchronized (object) {
						try {
							object.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						System.out.println("the object has been waken up!");
					}
				}
			}
		}).start();
		TestZkWatcher testZkWatcher = new TestZkWatcher();
		ZkWatcher watcher = testZkWatcher.new ZkWatcher(clientOne(), object);
		watcher.start();
	}

	private static CuratorFramework clientOne() {
		// zk 地址
		String connectString = "192.168.6.131:2181";
		// 连接时间 和重试次数
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
		client.start();
		return client;
	}

	class ZkWatcher extends Thread {
		private CuratorFramework client;
		private final Object object;

		public ZkWatcher(CuratorFramework client, Object object) {
			this.client = client;
			this.object = object;
		}

		public void run() {
			try {
				 setListenterThreeOne(client);
//				setListenterThreeTwo(client);
				// setListenterThreeThree(client);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void setListenterOne(final CuratorFramework client) throws Exception {
			// 注册观察者，当节点变动时触发
			byte[] data = null;
			try {
				data = client.getData().usingWatcher(new Watcher() {
					public void process(WatchedEvent event) {
						System.out.println("获取 watcher 节点 监听器 : " + event);
						try {
							System.out.println(new String(client.getData().forPath(event.getPath())));
						} catch (Exception e) {
							// TODO 自动生成的 catch 块
							e.printStackTrace();
						}
					}
				}).forPath("/test/watcher");
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("watcher 节点数据: " + new String(data));
			Thread.sleep(100000);
		}

		private void setListenterTwo(CuratorFramework client) throws Exception {
			ExecutorService pool = Executors.newCachedThreadPool();
			CuratorListener listener = new CuratorListener() {
				public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("监听器  : " + event.toString());
				}

			};
			client.getCuratorListenable().addListener(listener, pool);
			client.getData().inBackground().forPath("/test/watcher");
			client.getData().inBackground().forPath("/test/watcher");
			client.getData().inBackground().forPath("/test/watcher");
			client.getData().inBackground().forPath("/test/watcher");
			Thread.sleep(Long.MAX_VALUE);
		}

		private void setListenterThreeOne(CuratorFramework client) throws Exception {
			PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
				int tag = 0;
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					System.out.println("开始进行事件分析:-----");
					ChildData data = event.getData();
					switch (event.getType()) {
					case CHILD_ADDED:
						System.out.println("CHILD_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;
					case CHILD_REMOVED:
						System.out.println("CHILD_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;
					case CHILD_UPDATED:
						if(tag == 0){
							tag = 1;
							System.out.println("CHILD_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
							ZkUtils.setData("/test/watcher", "abc");
							Thread.sleep(2000);
							System.out.println(System.currentTimeMillis()/1000L);
							ZkUtils.setData("/test/watcher", "abcdasfasfasfas");
							Thread.sleep(5000);
						}else if (tag == 1){
							tag = 0;
						}
						break;
					default:
						break;
					}
				}
			};
			ExecutorService pool = Executors.newCachedThreadPool();
			PathChildrenCache childrenCache = new PathChildrenCache(client, "/test", true);
			childrenCache.getListenable().addListener(childrenCacheListener);
			System.out.println("Register zk watcher successfully!");
			childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
			// Thread.sleep(Long.MAX_VALUE);
		}

		private void setListenterThreeTwo(CuratorFramework client) throws Exception {
			// ExecutorService pool = Executors.newCachedThreadPool();
			// 设置节点的cache
			final NodeCache nodeCache = new NodeCache(client, "/test/watcher", false);
			final NodeCache nodeCache2 = new NodeCache(client, "/test/watcher2", false);
			NodeCacheListener listener = new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					System.out.println("the test node is change and result is :");
					System.out.println("path : " + nodeCache.getCurrentData().getPath());
					System.out.println("data : " + new String(nodeCache.getCurrentData().getData()));
					System.out.println("stat : " + nodeCache.getCurrentData().getStat());
					synchronized (object) {
						if (new String(nodeCache.getCurrentData().getData()).equals("watcher")) {
							object.notify();
							Thread.currentThread().interrupt();
						} else {
							System.out.println(Thread.currentThread().getName());
							if (Thread.currentThread().isInterrupted()) {
								System.out.println("Thread is interrupted!");
							}
						}
					}
				}
			};
			NodeCacheListener listener2 = new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					System.out.println("the test node is change and result is :");
					System.out.println("path : " + nodeCache2.getCurrentData().getPath());
					System.out.println("data : " + new String(nodeCache2.getCurrentData().getData()));
					System.out.println("stat : " + nodeCache2.getCurrentData().getStat());
					if (new String(nodeCache.getCurrentData().getData()).equals("watcher")) {
						object.notify();
						Thread.currentThread().interrupt();
					} else {
						System.out.println(Thread.currentThread().getName());
						if (Thread.currentThread().isInterrupted()) {
							System.out.println("Thread is interrupted!");
						}
					}
				}
			};
			nodeCache.getListenable().addListener(listener);
			nodeCache2.getListenable().addListener(listener2);
			nodeCache.start();
			nodeCache2.start();
			Thread.sleep(Long.MAX_VALUE);
		}

		// 3.Tree Cache
		// 监控 指定节点和节点下的所有的节点的变化--无限监听 可以进行本节点的删除(不在创建)
		private void setListenterThreeThree(CuratorFramework client) throws Exception {
			ExecutorService pool = Executors.newCachedThreadPool();
			// 设置节点的cache
			TreeCache treeCache = new TreeCache(client, "/test");
			// 设置监听器和处理过程
			treeCache.getListenable().addListener(new TreeCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
					ChildData data = event.getData();
					if (data != null) {
						switch (event.getType()) {
						case NODE_ADDED:
							System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
							break;
						case NODE_REMOVED:
							System.out
									.println("NODE_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
							break;
						case NODE_UPDATED:
							System.out
									.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
							break;

						default:
							break;
						}
					} else {
						System.out.println("data is null : " + event.getType());
					}
				}
			});
			// 开始监听
			treeCache.start();

		}
	}
}
