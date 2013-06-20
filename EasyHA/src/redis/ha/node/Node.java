package redis.ha.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Node{
	public Map status;
	
	public Node(){
		status = new ConcurrentHashMap();
	}
}
