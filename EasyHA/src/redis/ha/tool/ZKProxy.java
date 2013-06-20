package redis.ha.tool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import redis.ha.Constants;

import com.google.gson.Gson;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorFrameworkFactory.Builder;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;

public class ZKProxy {
	private static final Logger LOG = Logger.getLogger(ZKProxy.class);

	private CuratorFramework curator;
	private static Gson  json;

	public ZKProxy(CuratorFramework curator) {
		this.curator = curator;
	}
	
	public ZKProxy(String zkPath) {
		Builder builder = CuratorFrameworkFactory.builder()
				.connectString(zkPath)
				.retryPolicy(new ExponentialBackoffRetry(3000, 10))
				.namespace(Constants.NAMESPACE); 
		curator = builder.build();
		curator.start();
	}

	public List<String> list(String path) {
//		LOG.info("list abspath for path="+path);
		List<String> children = null;
		try {
			if(checkPath(path)){
				children = curator.getChildren().forPath(path);
				for(int i =0 ;i<children.size();i++){
					if(!path.equals("/")){
						children.set(i, path+"/"+children.get(i));
					}else{
						children.set(i, "/"+children.get(i));
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return children;
	}
	
	public String get(String path){
		byte[] data = null;
		try {
			//测试是否必须
			if(curator.checkExists().forPath(path) != null){
				data = curator.getData().forPath(path);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(data!=null){
			return new String(data);
		}
		return null;
	}

	public boolean create(String path) {
		try {
			EnsurePath ensurePath = new EnsurePath(path);
			ensurePath.ensure(curator.getZookeeperClient());
			
			ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), path);
			
			ensurePath.ensure(curator.getZookeeperClient());
			if(curator.checkExists().forPath(path) != null){
				return true;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean create(String path,byte[] data) {
		try {
			EnsurePath ensurePath = new EnsurePath(path);
			ensurePath.ensure(curator.getZookeeperClient());
			
			ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), path);
			ensurePath.ensure(curator.getZookeeperClient());
			if(curator.checkExists().forPath(path) != null){
				curator.setData().forPath(path, data);
				return true;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean delete(String path) {
		try {
			curator.delete().forPath(path);
			if(curator.checkExists().forPath(path) == null){
				return true;
			}else if(list(path)!=null&&list(path).size()!=0){
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean deleteAll(String path) {
		try {
			if(list(path)!=null){
				for(String p:list(path)){
					LOG.warn("delete path="+p);
					deleteAll(p);
				}
			}
			curator.delete().forPath(path);
			
			if(curator.checkExists().forPath(path) == null){
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean edit(String path,String data) {
		try {
			curator.setData().forPath(path, data.getBytes());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public void close(){
		curator.close();
	}
	
	static void printList(List<String> list){
		for(String path : list){
			System.out.println(path);
		}
	}
	
	public boolean checkPath(String path){
		try {
			if(curator.checkExists().forPath(path)!=null)
				return true;
		} catch (Exception e) {
			LOG.error("check path ["+path+"] error");
			e.printStackTrace();
			return false;
		}
		return false;
	}
	
	public void moveto(String pathfrom,String pathtoParent) throws Exception{
		System.out.println("ready to move from "+pathfrom+" to"+pathtoParent);
		String nodeName = pathfrom.substring(pathfrom.lastIndexOf("/"+1));
		if(curator.checkExists().forPath(pathfrom)!=null&&curator.checkExists().forPath(pathtoParent)!=null){
			byte[] data = curator.getData().forPath(pathfrom);
			if(data!=null){
				curator.create().forPath(StringUtil.getAbsolutePath(pathtoParent,nodeName),data);
			}else{
				curator.create().forPath(StringUtil.getAbsolutePath(pathtoParent,nodeName));
			}
			curator.delete().forPath(pathfrom);
			LOG.info("node "+pathfrom+" has been moved to "+pathtoParent);
		}
	}
	
    public static String toString(byte[] bytes) {
        if(bytes == null || bytes.length ==0) return null;
        return new String(bytes);
    }
    
    public static String toJson(Object obj){
    	return json.toJson(obj);
    }
    
    public static String fromJson(String data,Class cls){
    	return json.fromJson(data, cls);
    }

    public static byte[] toBytes(Serializable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream objout = new ObjectOutputStream(out);
        objout.writeObject(obj);
        return out.toByteArray();
    }
    
    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException{
        ObjectInputStream objin = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return objin.readObject();
    }
    
    public static void main(String[] args) {
		ZKProxy proxy = new ZKProxy("10.75.17.173:2181,10.75.17.174:2181,10.75.18.139:2181");
		List<String> list = proxy.list(Constants.LOCK+"/w499");
		if(list!=null){
			for (String path:list) {
				System.out.println(path);
			}
		}
		proxy.deleteAll(Constants.LOCK+"/w499");
	}
}
