package redis.ha;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import redis.ha.node.Node;
import redis.ha.strategy.RedisStrategy;
import redis.ha.strategy.Strategy;
import redis.ha.tool.ZooKeeperClient;

public class ServiceInstance implements Runnable{
	private static final Logger LOG = Logger.getLogger(ServiceInstance.class);
	private final Map conf = new ConcurrentHashMap();
	private final Map state = new ConcurrentHashMap();
	private Strategy strategy;
	
	Set<Node> nodes;
	public final String serviceID;
	public volatile boolean start = false;
	
	ServiceInstance(String serviceID){
		this.serviceID = serviceID;
	}

	/**
	 * 获取此service的最新状态
	 * @return
	 */
	public Map status(){
		return state;
	}
	
	public void prepare() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException{
		
		ZooKeeperClient client = (ZooKeeperClient) conf.get(Constants.CLIENT);
		String strategyClass = client.getStrategyName(serviceID);
		Constructor construtor = Class.forName(strategyClass).getConstructor(String.class,Map.class);
		strategy = (Strategy) construtor.newInstance(serviceID,conf);
		nodes = strategy.listNode();
	}
	
	/**
	 * 设置此service的config对象的keyvalue
	 * @param key
	 * @param value
	 */
	public void config(Object key,Object value){
		conf.put(key, value);
	}
	
	/**
	 * 根据serviceID构建node list
	 */
	void build(){
		LOG.info(String.format("build node for service,serviceID=%s",serviceID));
		strategy.build();
		nodes = strategy.listNode();
		if(nodes!=null){
			LOG.info("nodes is ready for monitoring,nodes count="+nodes.size());
		}else{
			System.exit(1);
		}
	}
	
	/**
	 * 在运行过程中动态的发现新添加的节点
	 */
	void discoverNode(){
		strategy.discover();
	}
	
	/**
	 * 周期性检查此service下的node是否有异常
	 */
	void check(){
		LOG.info(String.format("check serviceID=%s,node count=%s",serviceID,nodes.size()));
		strategy.check();
	}
	
	/**
	 * 发现node异常后的处理过程
	 */
	void failover(){
		
	}

	@Override
	public void run() {
		Config config = (Config) conf.get(Constants.CONFIG);
		LOG.info("service start monitor nodes ,serviceID="+serviceID);
		build();
		start = true;
		while(start){
			check();
			discoverNode();
			try {
				Thread.sleep(config.sleepDelay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
