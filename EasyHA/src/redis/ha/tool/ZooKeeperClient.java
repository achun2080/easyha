package redis.ha.tool;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import redis.clients.jedis.Jedis;
import redis.ha.Constants;
import redis.ha.ServiceInstance;
import redis.ha.node.Node;
import redis.ha.strategy.RedisStrategy;
import redis.ha.strategy.Strategy;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorFrameworkFactory.Builder;
import com.netflix.curator.retry.ExponentialBackoffRetry;

public class ZooKeeperClient {
	private static final Logger LOG = Logger.getLogger(ZooKeeperClient.class);

	private volatile boolean closed = false;
	public final int RETRYNUM = 10;
	public final int SESSIONTIMEOUT = 3000;

	public final CuratorFramework curator;
	public ZKProxy proxy;

	public ZooKeeperClient(String zkAddress) throws Exception {
		Builder builder = CuratorFrameworkFactory.builder().connectString(zkAddress)
				.retryPolicy(new ExponentialBackoffRetry(3000, RETRYNUM)).namespace(Constants.NAMESPACE);
		curator = builder.build();
		curator.start();

		proxy = new ZKProxy(curator);
	}

	public void close() {
		LOG.info("Closing CuratorZKClient");
		if (!this.closed) {
			this.closed = true;
			this.curator.close();
		}
	}

	public List<String> findNewService(Map<String, ServiceInstance> services) throws Exception {
		List<String> list = curator.getChildren().forPath("");
		List<String> newServices = new ArrayList<String>();

		for (String serviceID : list) {
			if (!services.containsKey(serviceID) && !Constants.CONFIG.equals(serviceID)
					&& !Constants.LOCK.equals(serviceID)) {
				newServices.add(serviceID);
			}
		}
		return newServices;
	}

	public Set<String> removeOldService(Map<String, ServiceInstance> services) throws Exception {
		List<String> list = curator.getChildren().forPath("");
		Set<String> serviceIDSet = new HashSet<String>();
		serviceIDSet.addAll(services.keySet());
		for (String serviceID : list) {
			if (services.containsKey(serviceID)) {
				serviceIDSet.remove(serviceID);
			}
		}
		if (serviceIDSet.size() !=0 ) {
			return serviceIDSet;
		}
		return null;
	}

	public boolean checkManagerIsStoped() {
		boolean flag = false;
		try {
			String data = new String(curator.getData().forPath(""));
			if (data.equals("stop")) {
				flag = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}
	
	public boolean startManager() {
		boolean flag = false;
		try {
			if(curator.setData().forPath("","start".getBytes())!=null){
				LOG.info("set start for path /" + Constants.NAMESPACE);
				flag=true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	public void stopManager() {
		try {
			curator.setData().forPath("", "stop".getBytes());
			LOG.info("set stop for path /" + Constants.NAMESPACE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// 根据serviceID构建其下node信息
	public Set<Node> buildByID(String serviceID) {
		LOG.info("zookeeper buildByID for serviceID=" + serviceID);
		Set<Node> nodes = new HashSet<Node>();

		try {
			// 检查offline目录是否为空，如果不为空则不实例化service
			List<String> nodePaths = proxy.list(StringUtil.getAbsolutePath(serviceID, Constants.OFFLINE));
			if (nodePaths != null && nodePaths.size() != 0) {
				LOG.error("service offline path have node,please failover manually! node size=" + nodePaths.size());
				return null;
			}

			// 从online实例化节点
			nodePaths = proxy.list(StringUtil.getAbsolutePath(serviceID, Constants.ONLINE));
			if (nodePaths != null && nodePaths.size() != 0) {
				for (String path : nodePaths) {
					Node node = new Node();
					String[] nodeInfo = proxy.get(path).split(",");
					if (nodeInfo != null) {
						for (String record : nodeInfo) {
							String[] kv = record.split(":", 2);
							node.status.put(kv[0], kv[1]);// 读取idc、ip、port、role信息
						}
						nodes.add(node);
					}
				}
				LOG.info("service online path have node,node size=" + nodePaths.size() + ",add success!");
			}

			// 从init目录中实例化节点
			nodePaths = proxy.list(StringUtil.getAbsolutePath(serviceID, Constants.INIT));
			if (nodePaths != null && nodePaths.size() != 0) {
				for (String path : nodePaths) {

					Node node = new Node();
					String[] nodeInfo = proxy.get(path).split(",");
					if (nodeInfo != null) {
						for (String record : nodeInfo) {
							String[] kv = record.split(":", 2);
							node.status.put(kv[0], kv[1]);// 读取idc、ip、port、role信息
						}
						nodes.add(node);
					}
					// TODO MOVE path
					proxy.moveto(path, StringUtil.getAbsolutePath(serviceID, Constants.ONLINE));
				}
				LOG.info("service init path have node,node size=" + nodePaths.size() + ",add success!");
			}
			return nodes;
		} catch (Exception e) {
			e.printStackTrace();
			nodes = null;
			LOG.info("error occur when build path,all nodes is removed");
		}
		return null;
	}

	/**
	 * 1.添加offlinenode 2.删掉onlinenode
	 * 
	 * @param serviceID
	 * @param node
	 * @throws Exception
	 */
	public void offlineNode(String serviceID, Node node) throws Exception {
		String onlineNodePath = StringUtil.getAbsolutePath(serviceID, Constants.ONLINE, node.status.get("ip") + ":"
				+ node.status.get("port"));
		String offlineNodePath = StringUtil.getAbsolutePath(serviceID, Constants.OFFLINE, node.status.get("ip") + ":"
				+ node.status.get("port"));
		byte[] data = curator.getData().forPath(onlineNodePath);
		curator.create().forPath(offlineNodePath, data);
		if (curator.checkExists().forPath(offlineNodePath) != null) {
			curator.delete().forPath(onlineNodePath);
		}
	}

	public Set<Node> discoverInitNode(String serviceID) throws Exception {
		Set<Node> nodes = new HashSet<Node>();

		// 从init目录中实例化节点
		List<String> nodePaths = proxy.list(StringUtil.getAbsolutePath(serviceID, Constants.INIT));
		if (nodePaths != null && nodePaths.size() != 0) {
			for (String path : nodePaths) {

				Node node = new Node();
				String[] nodeInfo = proxy.get(path).split(",");
				if (nodeInfo != null) {
					for (String record : nodeInfo) {
						String[] kv = record.split(":", 2);
						node.status.put(kv[0], kv[1]);// 读取idc、ip、port、role信息
					}
					nodes.add(node);
				}
				// TODO MOVE path
				proxy.moveto(path, StringUtil.getAbsolutePath(serviceID, Constants.ONLINE));
			}
			LOG.info("service init path have node,node size=" + nodePaths.size() + ",add success!");
		}
		return nodes;
	}
	
	//从online目录中获取节点并比较是否和service管理的数量不同，小于的话则remove掉service管理的多余部分
	public Set<Node> checkOnlineNode(String serviceID,Set<Node> nodes) throws Exception {
		Set<Node> onlines = new HashSet<Node>();

		// 从online目录中实例化节点
		List<String> nodePaths = proxy.list(StringUtil.getAbsolutePath(serviceID, Constants.ONLINE));
		if (nodePaths != null && nodePaths.size() != 0) {
			for (String path : nodePaths) {

				Node node = new Node();
				String[] nodeInfo = proxy.get(path).split(",");
				if (nodeInfo != null) {
					for (String record : nodeInfo) {
						String[] kv = record.split(":", 2);
						node.status.put(kv[0], kv[1]);// 读取idc、ip、port、role信息
					}
					onlines.add(node);
				}
			}
//			System.out.println("onlines="+onlines.size()+",nodes="+nodes.size());
			if(onlines.size()<nodes.size()){
				List<Node> tmp = new ArrayList<Node>();
				tmp.addAll(nodes);
				for(Node a:onlines){
					Iterator iter = tmp.iterator();
					while(iter.hasNext()){
						Node b = (Node) iter.next();
						int aIP = StringUtil.IP2Integer((String) a.status.get("ip"));
						int aPort = Integer.valueOf((String) a.status.get("port"));
						int bIP = StringUtil.IP2Integer((String) b.status.get("ip"));
						int bPort = Integer.valueOf((String) b.status.get("port"));
						if(aIP==bIP&&aPort==bPort){
							iter.remove();
						}
					}
				}
				Iterator iter = nodes.iterator();
				while(iter.hasNext()){
					Node a = (Node) iter.next();
					for(Node b: tmp){
						int aIP = StringUtil.IP2Integer((String) a.status.get("ip"));
						int aPort = Integer.valueOf((String) a.status.get("port"));
						int bIP = StringUtil.IP2Integer((String) b.status.get("ip"));
						int bPort = Integer.valueOf((String) b.status.get("port"));
						
						if(aIP==bIP&&aPort==bPort){
							iter.remove();
						}
					}
				}
				LOG.info("service delete node success!");
			}
		}
		return nodes;
	}
	
	public Set<Node> checkOfflineNode(String serviceID) {
		Set<Node> nodes = new HashSet<Node>();
		// 从offline目录中实例化节点
		List<String> nodePaths = proxy.list(StringUtil.getAbsolutePath(serviceID, Constants.OFFLINE));
		if (nodePaths != null && nodePaths.size() != 0) {
			for (String path : nodePaths) {
				Node node = new Node();
				String[] nodeInfo = proxy.get(path).split(",");
				if (nodeInfo != null) {
					for (String record : nodeInfo) {
						String[] kv = record.split(":", 2);
						node.status.put(kv[0], kv[1]);// 读取idc、ip、port、role信息
					}
					Jedis jedis = new Jedis((String) node.status.get("ip"),Integer.valueOf((String) node.status.get("port")));

					long latency = -1;
					try {
						long start = System.currentTimeMillis();
						jedis.ping();
						long end = System.currentTimeMillis();
						latency = end - start;
					} catch (Exception e) {
						LOG.error("node still can't be connected,node="+printNode(node));
						e.printStackTrace();
					}
					if (latency >= 0) {
						clearRedisInfo(node); // 清除redisnode info 信息
						updateRedisInfo(node); // 更新redisnode info 信息
						String role = (String) ((Map) node.status.get("redisInfo")).get("role");
						String masterLinkStatus = (String) ((Map) node.status.get("redisInfo")).get("master_link_status");
						
						if (role.equals("slave") && masterLinkStatus.equals("up")) {
							nodes.add(node);
						}
					}
				}
			}
		}
		return nodes;
	}
	
	public long ping(Jedis jedis) {
		long latency = -1;
		long start = System.currentTimeMillis();
		jedis.ping();
		long end = System.currentTimeMillis();
		latency = end - start;
		return latency;
	}
	
	public String printNode(Node node) {
		return node.status.get("ip") + ":" + node.status.get("port");
	}
	
	public void clearRedisInfo(Node node) {
		Map redisInfo = (Map) node.status.get("redisInfo");
		if (redisInfo == null) {
			redisInfo = new HashMap<String, String>();
			node.status.put("redisInfo", redisInfo);
		}
		redisInfo.clear();
	}

	public void updateRedisInfo(Node node) {
		Jedis jedis = (Jedis) node.status.get("jedis");
		if(jedis==null){
			jedis = new Jedis((String)node.status.get("ip"),Integer.valueOf((String)node.status.get("port")));
			node.status.put("jedis", jedis);
		}
		Map redisInfo = (Map) node.status.get("redisInfo");
		try {
			String[] infolist = jedis.info().split("\n");
			redisInfo = StringUtil.extractRedisInfo(Arrays.asList(infolist));
			node.status.put("redisInfo", redisInfo);
		} catch (Exception e) {
			LOG.error(String.format("update redis %s -info error", printNode(node)));
		}
	}
	
	public void onlineNode(String path,String serviceID) throws Exception{
		proxy.moveto(path, StringUtil.getAbsolutePath(serviceID, Constants.ONLINE));
	}
	
	public String getStrategyName(String serviceID){
		String strategyName = proxy.get(serviceID);
		return strategyName;
	}
	
	public void setStrategyName(String serviceID,String cls) throws Exception{
		curator.setData().forPath(serviceID,cls.getBytes());
	}
	
	public static void main(String[] args) throws Exception {
//		ZooKeeperClient client = new ZooKeeperClient("10.75.17.173:2181,10.75.17.174:2181,10.75.18.139:2181");
//		client.setStrategyName("w476", "redis.ha.strategy.RedisStrategy");
		
		Constructor construtor = Class.forName("redis.ha.strategy.RedisStrategy").getConstructor(String.class,Map.class);
		RedisStrategy strategy = (RedisStrategy) construtor.newInstance("w476",new HashMap());
		System.out.println(strategy);
	}
}
