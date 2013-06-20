package redis.ha;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import redis.ha.Constants;

import com.netflix.curator.framework.recipes.leader.LeaderLatch;

import redis.ha.tool.ZooKeeperClient;

/*
 * 
 */
public class ManagerImpl implements Manager{
	private static final Logger LOG = Logger.getLogger(ManagerImpl.class);
	public ExecutorService servicePool;
	private ZooKeeperClient client;
	private LeaderLatch latch;
	protected Config config;
	private volatile boolean start=false;
	
	private Map<String,ServiceInstance> services;
	
	public ManagerImpl(String zkPath) throws Exception {
		LOG.info(String.format("Manager constructor:zk=%s",zkPath));
		this.servicePool = Executors.newFixedThreadPool(50);
		services = new ConcurrentHashMap<String,ServiceInstance>();
		
		client = new ZooKeeperClient(zkPath);
		config = new Config(client.curator).get();
		
		//锁地址 /RedisCluster/lock
		latch = new LeaderLatch(client.curator, Constants.LOCK);
		latch.start();
	}
	
	/**
	 * 周期性检查根目录下是否有新的serviceID生成
	 */
	@Override
	public void discoverService(){
		try {
			List<String> newServices = client.findNewService(services);
			if(newServices.size()!=0){
				for(String serviceID : newServices){
					registerService(serviceID);
				}
			}
			
			//移除已被删除的service
			Set<String> oldServices = client.removeOldService(services);
			if(oldServices!=null&&oldServices.size()!=0){
				for(String serviceID:oldServices){
					System.out.println("for loop ="+serviceID);
					unregisterService(serviceID);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void registerService(String serviceID){
		ServiceInstance service = new ServiceInstance(serviceID);
		service.config(Constants.CONFIG, config);
		service.config(Constants.CLIENT, client);
		try {
			service.prepare();
			services.put(serviceID, service);
			LOG.info("discover new service,service ID="+serviceID);
			servicePool.submit(service);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unregisterService(String serviceID) {
		ServiceInstance service = services.get(serviceID);
		if(service!=null){
			service.start=false;
			services.remove(serviceID);
			LOG.info("remove service from manager ,service ID="+serviceID);
		}
	}

	@Override
	public ServiceInstance queryForInstance(String serviceID) {
		ServiceInstance service = services.get(serviceID);
		return service;
	}

	@Override
	public void start() {
		while (!latch.hasLeadership()) {
			LOG.info("waiting for becaming master manager");
			try {
				Thread.sleep(config.sleepDelay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOG.info("starting manager");
		start=true;
		if(!client.startManager()){
			LOG.error("can't set start flag for ha namespace ,return");
			return;
		}
//		discoverService();
//		if(services.size()!=0){
//			for(ServiceInstance serive:services.values()){
//				servicePool.submit(serive);
//			}
//		}
		
		while(start){
			stop();
			discoverService();
			try {
				LOG.info("manager loop monit,sleepDelay="+config.sleepDelay);
				Thread.sleep(config.sleepDelay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}finally{
				
			}
		}
		LOG.info("manager has been shutdown");
	}
	
	/**
	 * check the / path ，if the data value equals stop，then stop mananger
	 */
	@Override
	public void stop() {
		if(client.checkManagerIsStoped()){
			LOG.info("manager is closing");
			for(ServiceInstance service :services.values()){
				service.start = false;
				LOG.info("service ["+service.serviceID+"] is closing");
			}
			start=false;
			servicePool.shutdown();
			while(!servicePool.isShutdown()){
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			client.close();
		}
	}
	
	@Override
	public void shutdown(){
		client.stopManager();
	}
	
	public static void main(String[] args) throws Exception {

		if(args.length!=2||args[0].equals("")){
			System.out.println("usage: zkpath [start/stop]");
			return;
		}
//		Manager manager = new ManagerImpl("10.75.17.173:2181,10.75.17.174:2181,10.75.18.139:2181");	
		
		if(args[1].equals("stop")){
			Manager manager = new ManagerImpl(args[0]);
			manager.shutdown();
		}else if(args[1].equals("start")){
			Manager manager = new ManagerImpl(args[0]);
			manager.start();
		}
	}
}
