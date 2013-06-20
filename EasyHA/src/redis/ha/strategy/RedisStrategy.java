package redis.ha.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.ha.Config;
import redis.ha.Constants;
import redis.ha.ServiceInstance;
import redis.ha.node.Node;
import redis.ha.tool.DNSTool;
import redis.ha.tool.StringUtil;
import redis.ha.tool.ZooKeeperClient;

public class RedisStrategy implements Strategy {
	private static final Logger LOG = Logger.getLogger(RedisStrategy.class);
	public Set<Node> nodes;
	private ZooKeeperClient client;
	protected final Config config;
	private final String serviceID;

	public RedisStrategy(String serviceID, Map config) {
		this.serviceID = serviceID;
		this.client = (ZooKeeperClient) config.get(Constants.CLIENT);
		this.config = (Config) config.get(Constants.CONFIG);
		nodes = new HashSet<Node>();
	}

	@Override
	public void build() {
		nodes = client.buildByID(serviceID);
		// unique();
	}

	@Override
	public boolean check() {
		boolean isOK = true;
		Iterator<Node> iter = nodes.iterator();
		while (iter.hasNext()) {
			Node node = iter.next();
			if (node.status.get("role").equals("slave")) {
				isOK = checkSlaveISOK(node);
				if(node.status.get("currentErrorCount")==null){
					node.status.put("currentErrorCount",0);
				}
				if (!isOK && (Integer) node.status.get("currentErrorCount") >= config.maxErrors) {

					failover(node);
					iter.remove();
					LOG.info("node " + printNode(node) + "is removed from online nodes list");
				}
			}
		}
		return isOK;
	}

	/**
	 * 1.检查init目录是否有新节点加入 2.检查offline目录是否有节点恢复运行
	 */
	@Override
	public void discover() {
		try {
			// 检查offline目录是否有节点恢复运行
			Set<Node> offNodes = client.checkOfflineNode(serviceID);
			if (offNodes.size() != 0) {
				nodes.addAll(offNodes);

				for (Node node : offNodes) {
					String path = StringUtil.getAbsolutePath(serviceID, Constants.OFFLINE, node.status.get("ip") + ":"
							+ node.status.get("port"));
					client.onlineNode(path, serviceID);
					LOG.info("node " + printNode(node) + " is ready for service ,node path is moved from " + path
							+ " to online");
					Node master = getMaster(node);
					if (master != null) {
						DNSTool.moveback(master, node);
						LOG.info("node " + printNode(node) + " dns is change from" + printNode(master) + " back to "
								+ node);
					}

				}
			}

			// 检查init目录是否有新节点加入
			Set<Node> initNodes = client.discoverInitNode(serviceID);
			if (initNodes.size() != 0) {
				LOG.info("discover new node for service=" + serviceID + " node count=" + initNodes.size());
				for (Node node : initNodes) {
					if ("master".equals(node.status.get("role"))) {
						nodes.add(node);
					} else if ("slave".equals(node.status.get("role"))) {
						Node master = getMaster(node);
						if (master != null) {
							nodes.add(node);
						} else {
							String nodePath = StringUtil.getAbsolutePath(serviceID, Constants.ONLINE,
									(String) node.status.get("ip") + ":" + (String) node.status.get("port"));
							client.proxy.moveto(nodePath, StringUtil.getAbsolutePath(serviceID, Constants.INIT));
							LOG.error("please check added node [" + printNode(node)
									+ "],maybe it don't have redis master");
						}
					}
				}
			}
			// TODO 检查online目录是否节点删除
			// System.out.println("nodes before="+nodes.size());
			client.checkOnlineNode(serviceID, nodes);
			// System.out.println("nodes after="+nodes.size());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 1.判断节点是否故障 2.获取失败节点的master节点 3.设置cname 4.从nodes删除失败节点
	 */
	@Override
	public void failover(Node node) {
		boolean isOK = true;
		if (node.status.get("role").equals("slave")) {
			Jedis jedis = (Jedis) node.status.get("jedis");
			long latency = -1;
			try {
				long start = System.currentTimeMillis();
				jedis.ping();
				long end = System.currentTimeMillis();
				latency = end - start;
			} catch (Exception e) {
				if (node.status.get("ip") == null || node.status.get("port") == null) {
					LOG.error("ping error while failover, and node can't be created");
				} else {
					LOG.error("ping error while failover,host:" + node.status.get("ip") + ":" + node.status.get("port"));
				}
			}
			String Role = (String) ((Map) node.status.get("redisInfo")).get("role");
			String MasterLinkStatus = (String) ((Map) node.status.get("redisInfo")).get("master_link_status");

			if (latency == -1 || Role == null || !Role.equals("slave") || MasterLinkStatus == null
					|| !MasterLinkStatus.equals("up")) {
				System.out.println("master host:" + ((Map) node.status.get("redisInfo")).get("master_host"));
				System.out.println("master port:" + ((Map) node.status.get("redisInfo")).get("master_port"));
				for (Node master : nodes) {
					if (!master.status.get("ip").equals(node.status.get("ip"))
							&& master.status.get("port").equals(node.status.get("port"))) {
						LOG.info("node loss connection, dns will change node cname to it's master");
						DNSTool.failover(node, master);
						LOG.info("dns change from " + printNode(node) + " to " + printNode(master));

						try {
							client.offlineNode(serviceID, node);
							LOG.info("node=" + printNode(node) + "in zk path is moved to offline");
						} catch (Exception e) {
							LOG.error("offline node fail,node=" + printNode(node));
						}
					}
				}
			}
		}
	}

	@Override
	public Set<Node> listNode() {
		return nodes;
	}

	@Override
	public Node getNode(String id) {
		return null;
	}

	@Override
	public void addNode(Node node) {
		nodes.add(node);
	}

	@Override
	public void removeNode(Node node) {

	}

	public void unique() {
		List repeatedNode = new ArrayList();
		if (nodes != null) {
			List nodeList = new ArrayList(nodes);
			for (int i = 0; i < nodeList.size(); i++) {
				Node compareA = (Node) nodeList.get(i);
				for (int j = i + 1; j < nodeList.size(); j++) {
					Node compareB = (Node) nodeList.get(j);
					if (compareA.status.get("ip").equals(compareB.status.get("ip"))
							&& compareA.status.get("port").equals(compareB.status.get("port"))) {
						repeatedNode.add(compareA);
					}
				}
			}
			nodes.removeAll(repeatedNode);
			LOG.info("when building node ,serivce contain repeat node,node size=" + repeatedNode.size());
		}
	}

	private boolean checkSlaveISOK(Node redis) {
		if(!checkMasterISOK(redis)){
			return true;
		}
		Jedis jedis = (Jedis) redis.status.get("jedis");
		if (jedis == null) {
			jedis = new Jedis((String) redis.status.get("ip"), Integer.valueOf((String) redis.status.get("port")));
			redis.status.put("jedis", jedis);
		}
		Integer currentErrorCount = 0;
		currentErrorCount = (Integer) redis.status.get("currentErrorCount");
		if (currentErrorCount == null) {
			currentErrorCount = 0;
			redis.status.put("currentErrorCount", currentErrorCount);
		}
		try {
			long latency = ping(jedis);
			if (latency == -1) {
				redis.status.put("jedis",
						new Jedis((String) redis.status.get("ip"), Integer.valueOf((String) redis.status.get("port"))));
				latency = ping(jedis);
			}
			if (latency < config.pingLatency && latency >= 0) {

				clearRedisInfo(redis); // 清除redisnode info 信息
				updateRedisInfo(redis); // 更新redisnode info 信息

				String role = (String) ((Map) redis.status.get("redisInfo")).get("role");
				String masterLinkStatus = (String) ((Map) redis.status.get("redisInfo")).get("master_link_status");
				if (role.equals("slave") && masterLinkStatus.equals("up")) {
					redis.status.put("currentErrorCount", 0);
					String domain = StringUtil.getDomain(redis);
					if (domain != null && !domain.equals("") && checkSlaveDomain(redis, getMaster(redis))) {
						redis.status.put("domain", domain);
						return true;
					}
				} else {
					// XXX 如果master故障，不累加counter。如果slave没有完全启动，不累加错误
					++currentErrorCount;
					redis.status.put("currentErrorCount", currentErrorCount);
					LOG.info(printNode(redis) + ",master or slave doesn't startup!");
				}
			} else {
				++currentErrorCount;
				redis.status.put("currentErrorCount", currentErrorCount);
			}
		} catch (Exception e) {
			e.printStackTrace();
			++currentErrorCount;
			redis.status.put("currentErrorCount", currentErrorCount);
			redis.status.put("jedis", initJedis(redis));

			LOG.error(String.format("Failed to talk to redis %s - error count is %s ", printNode(redis),
					redis.status.get("currentErrorCount")));
		}

		return false;
	}
	
	//TODO
	private boolean checkMasterISOK(Node node){
		// 获取master状态
		Node master = getMaster(node);
		if(master==null){
			LOG.info(String.format("%s master is null", printNode(node)));
			return false;
		}
		clearRedisInfo(master);
		updateRedisInfo(master);
		String mRole = (String) ((Map) master.status.get("redisInfo")).get("role");
		
		// master正常才继续检查
		if (mRole == null || !mRole.equals("master")) {
			return false;
		}
		return true;
	}

	private boolean checkSlaveDomain(Node redis, Node master) {
		return DNSTool.checkDomain(redis, master);
	}

	public long ping(Jedis jedis) {
		long latency = -1;
		long start = System.currentTimeMillis();
		jedis.ping();
		long end = System.currentTimeMillis();
		latency = end - start;
		return latency;
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
		if (jedis == null) {
			jedis = initJedis(node);
			node.status.put("jedis", jedis);
		}
		Map redisInfo = (Map) node.status.get("redisInfo");
		try {
			String[] infolist = jedis.info().split("\n");
			redisInfo = StringUtil.extractRedisInfo(Arrays.asList(infolist));
			node.status.put("redisInfo", redisInfo);
			
		} catch (Exception e) {
			node.status.put("jedis", initJedis(node));
			LOG.error(String.format("update redis %s -info error", printNode(node)));
		}
	}

	public String printNode(Node node) {
		return node.status.get("ip") + ":" + node.status.get("port") + ",idc=" + node.status.get("idc") + ",role="
				+ node.status.get("role");
	}

	public Node getMaster(Node slave) {
		String slaveIP = (String) slave.status.get("ip");
		String slavePort = (String) slave.status.get("port");
		try {
			for (Node master : nodes) {
				String ip = (String) master.status.get("ip");
				String port = (String) master.status.get("port");
				if (!ip.equals(slaveIP) && port.equals(slavePort)) {
					updateRedisInfo(master);
					Map redisInfo = (Map) master.status.get("redisInfo");
					if (redisInfo != null) {
						String role = (String) redisInfo.get("role");
						if(role!=null){
							if (role.equals("master")) {
								Jedis jedis = (Jedis) master.status.get("jedis");
								if (jedis != null) {
									long latency = ping(jedis);
									if (latency >= 0) {
										return master;
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			LOG.error("can't connect to node's master ,node="+printNode(slave));
		}
		return null;
	}

	public Jedis initJedis(Node node) {
		Jedis jedis = new Jedis((String) node.status.get("ip"), Integer.valueOf((String) node.status.get("port")));
		return jedis;
	}
}
