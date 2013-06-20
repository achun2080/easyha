package redis.ha;

public interface Manager {
	
	//从ZK指定路径中查找是否有新服务初始化
	void discoverService();
	
	//将新服务注册到manager中
	void registerService(String serviceID);
	
	//从ZK中删除指定服务
	void unregisterService(String serviceID);
	
	//根据ID获取服务实例
	ServiceInstance queryForInstance(String serviceID);
	
	void start();
	
	void stop();

	void shutdown();

}
