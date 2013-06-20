package redis.ha;

import java.io.Serializable;

import org.apache.log4j.Logger;

import redis.ha.Constants;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorFrameworkFactory.Builder;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.EnsurePath;

public class Config implements Serializable{
	private transient static final Logger LOG = Logger.getLogger(Config.class);
	
	public int sessiontimeout;
	public int connectiontimeout;
	public int maxActive;
	public int maxIdle;
	public int maxWait;
	public boolean testOnBorrow;
	public int maxErrors;
	public long sleepDelay;
	public long pingLatency;
	
	public transient CuratorFramework curator;
	private transient String configPath;
	private transient Gson json = new Gson();
	
	/**
	 * configPath must start with /
	 * @param curator
	 * @param configPath
	 */
	public Config(CuratorFramework curator) {
		
		this.curator = curator;
		this.configPath = "/" + Constants.CONFIG;
		EnsurePath path = new EnsurePath(configPath);
		
		try {
			if(curator.checkExists().forPath(configPath)==null){
				path.ensure(curator.getZookeeperClient());
				curator.create().forPath(configPath,"".getBytes());
				path.ensure(curator.getZookeeperClient());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void set() throws Exception{
		if(this.sessiontimeout==0){
			this.sessiontimeout = 30000;
		}
		if(this.connectiontimeout==0){
			this.connectiontimeout = 30000;
		}
		if(this.maxActive==0){
			this.maxActive = 100;
		}
		if(this.maxIdle==0){
			this.maxIdle = 5;
		}
		if(this.maxWait==0){
			this.maxWait = 1000;
		}
		if(this.testOnBorrow==false){
			this.testOnBorrow = true;
		}
		if(this.sleepDelay==0){
			this.sleepDelay = 10000;
		}
		if(this.maxErrors==0){
			this.maxErrors = 5;
		}
		if(this.pingLatency==0){
			this.pingLatency = 200;
		}
		String data = json.toJson(this);
		System.out.println("data set="+data);
		curator.setData().forPath(configPath,data.getBytes());
		LOG.info("Config set data to zk,data="+data);
	}
	
	public Config get(){
		try {
			if(curator.checkExists().forPath(configPath)!=null){
				String data = new String(curator.getData().forPath(configPath));
				if(data!=null&&!"".equals(data)){
					Config config = json.fromJson(data, Config.class);
					LOG.info("Config get data from zk,data="+data);
					return config;
				}
			}
			set();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				set();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return this;
	}
	
	public String echo() {
		String echo = "Config [sessiontimeout=" + sessiontimeout + ",connectiontimeout="
				+ connectiontimeout + ",maxActive=" + maxActive + ",maxIdel=" + maxIdle + ",maxWait=" + maxWait
				+ ",testOnBorrow=" + testOnBorrow + ",maxErrors=" + maxErrors
				+ ",sleepDelay=" + sleepDelay + ",pingLatency=" + pingLatency ;
		return echo;
	}
	
	public static void main(String[] args) throws Exception {
		
		Builder builder = CuratorFrameworkFactory.builder()
				.connectString("10.75.17.173:2181,10.75.17.174:2181,10.75.18.139:2181")
				.retryPolicy(new ExponentialBackoffRetry(10, 3000))
				.namespace(Constants.NAMESPACE); 
		CuratorFramework curator = builder.build();
		curator.start();
		
		Config config = new Config(curator);
		config.set();
		System.out.println(config.get().echo());
		config.curator.close();
	}
}
