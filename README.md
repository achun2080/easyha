easyha
======

隔离业务逻辑和HA基本功能，通过使用通用api完成对不同类型软件的监控。同时使用跨机房部署的ZooKeeper集群保障数据的安全可靠。

##总体架构
###Manager
EasyHA的主功能模块，从zookeeper中初始化需要监控的业务相关信息， 管理多个业务及配置信息。
###Service
是实际业务在EasyHA中的映射，包含业务配置信息和资源列表，周期性检测业务相关资源可用性，调用具体策略接口对异常资源进行恢复。
不同类型的软件由不同的策略进行管理，策略需要实现指定接口完成对某个软件进行监控。Service通过接口对不同类型的软件进行监控。
###Node
包含对某个业务中特定资源的状态信息。
###ZooKeeper
包含EasyHA的状态数据、锁数据、Service的资源数据。

###目录结构

>/rediscluster -------------ha的根目录

>/config	   -------------ha的系统配置项

>/lock      -------------ha锁信息，获得锁的ha提供服务其他则standby

>/serviceID ------------ 对应一个service对象，值为动态平台管理中心里的DB产品单元管理中的业务ID

>>/online		--------在线提供服务的资源列表
			
>>/offline    --------故障不能提供服务的资源列表
			
>>/initialize --------初始化待监控的资源列表
			
>>/ip:port	--------对应一个Node实例


##Strategy
EasyHA通过实现不同的Strategy策略接口可以对不同软件进行监控和failover处理。

Strategy接口包括如下方法：

void addNode(Node node);   //为service添加一个待监控Node

void removeNode(Node node);   //从监控列表中删除一个Node

Node getNode(String id);   // 根据ID获取一个Node

Set<Node> listNode();    //列举此service中所有Node

void build();    //初始化service

void discover();     //制定发现新node的方法

boolean check();    //周期性的检查node

void failover(Node node);    //check中发现错误节点后的相应处理规则
