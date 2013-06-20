package redis.ha.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.apache.log4j.Logger;
import org.junit.Test;

import redis.ha.Constants;
import redis.ha.node.Node;

public class DNSTool {
	private static final Logger LOG = Logger.getLogger(DNSTool.class);

	//是否删除旧IP，删除是否会删除单元
	public static boolean failover(Node from,Node to) {
		boolean flag = false;
		String domainFrom = StringUtil.getDomain((String)from.status.get("idc"), (String)from.status.get("ip"), (String)from.status.get("port"), (String)from.status.get("role"));
		String domainTo = StringUtil.getDomain((String)to.status.get("idc"), (String)to.status.get("ip"), (String)to.status.get("port"), (String)to.status.get("role"));
		String param ="";
		String[] ips = StringUtil.domain2IP(domainFrom);
		if(ips==null){
			LOG.error(domainFrom+" can't get ip");
			return false;
		}
		for(String ip:ips){
			if(((String)from.status.get("ip")).equals(ip)){
				param = paramBuild(domainFrom, (String)from.status.get("ip"), "A", "del");
				flag = post(param);
				param =  paramBuild(domainFrom, domainTo, "CNAME", "add");
				flag = post(param);
			}
		}
		return flag;
	}
	
	public static boolean moveback(Node from,Node to){
		boolean flag = false;
		String domainFrom = StringUtil.getDomain((String)from.status.get("idc"), (String)from.status.get("ip"), (String)from.status.get("port"), (String)from.status.get("role"));
		String domainTo = StringUtil.getDomain((String)to.status.get("idc"), (String)to.status.get("ip"), (String)to.status.get("port"), (String)to.status.get("role"));
		String param ="";
		String[] ips = StringUtil.domain2IP(domainFrom);
		if(ips!=null){
			for(String ip:ips){
				if(((String)from.status.get("ip")).equals(ip)){
					param = paramBuild(domainTo, domainFrom, "CNAME", "del");
					flag = post(param);
					param =  paramBuild(domainTo, (String)to.status.get("ip"), "A", "add");
					flag = post(param);
				}
			}
		}else{
			param = paramBuild(domainTo, domainFrom, "CNAME", "del");
			flag = post(param);
			param =  paramBuild(domainTo, (String)to.status.get("ip"), "A", "add");
			flag = post(param);
		}
		
		return flag;
	}
	
	@Test
	public void test(){
		System.out.println(post(paramBuild("rs8082.eos.grid.sina.com.cn","rs8082.eos.grid.sina.com.cn","CNAME","del")));
		System.out.println(post(paramBuild("rs8082.eos.grid.sina.com.cn","10.75.20.210","A","add")));
	}
	
	public static boolean checkDomain(Node node,Node master){
		String domain = (String)node.status.get("domain");
		if(domain==null||domain.equals("")){
			domain = StringUtil.getDomain(node);
			node.status.put("domain", domain);
		}
		String[] ips = StringUtil.domain2IP(domain);
		if(ips==null){
			DNSTool.moveback(master, node);
			return true;
		}
		for(String ip :ips){
//			System.out.println(ip+"~~"+node.status.get("ip"));
			if(ip.equals(node.status.get("ip"))){
				return true;
			}
		}
		return false;
	}
	
	public static boolean post(String param){
		String result = "";
		try {
			URL httpurl = new URL(Constants.URL);
			HttpURLConnection httpConn = (HttpURLConnection) httpurl.openConnection();
			httpConn.setDoOutput(true);
			httpConn.setDoInput(true);
			PrintWriter out = new PrintWriter(httpConn.getOutputStream());
			out.print(param);
			out.flush();
			out.close();
			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
			in.close();
			LOG.info(result);
		} catch (Exception e) {
			System.out.println("没有结果！" + e);
		}
		if(result!=null && !"".equals(result) && result.contains("succeed")){
			return true;
		}
		return false;
	}

	public static String paramBuild(String fromDomain,String toIP,String rtype,String op){
		StringBuilder sb = new StringBuilder(); 
		sb.append("domain=").append(fromDomain).append("&record=").append(toIP).append("&adminsys=dp_admin&method=").append(op).append("&rtype=").append(rtype);
		return sb.toString();
	}
	
	
	
//	public static void main(String[] args) throws Exception {
//		DNSTool tool = new DNSTool();
//		// tool.change("http://10.75.20.174:6790/api/","domain=rm9981.eos.grid.sina.com.cn&record=10.75.17.189&rtype=A&method=add&adminsys=dp_admin");
//	}
}
