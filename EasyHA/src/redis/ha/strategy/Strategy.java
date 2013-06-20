package redis.ha.strategy;

import java.util.List;
import java.util.Set;

import redis.ha.node.Node;

public interface Strategy {
	Set<Node> listNode();
	
	Node getNode(String id);
	
	void addNode(Node node);
	
	void removeNode(Node node);
	
	void build();
	
	boolean check();

	void discover();
	
	void failover(Node node);
	
}
