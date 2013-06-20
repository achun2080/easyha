package redis.ha.tool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import redis.ha.node.Node;

public class StringUtil {

	// TODO check 0
	public static int getPort(String dns) {
		int port = 0;
		Pattern pattern = Pattern.compile("\\d{4}");
		Matcher matcher = pattern.matcher(dns);
		if (matcher.find()) {
			port = Integer.parseInt(matcher.group(0));
		}
		return port;
	}

	public static String getKey(String ip, int port) {
		return ip + ":" + port;
	}

	public static String getDomain(String idc,String ip,String port,String role){
		String prefix = role.equals("master")?"rm":"rs";
		return prefix+port+"."+place2idc(idc)+".grid.sina.com.cn";
	}
	
	public static String getDomain(Node node){
		return getDomain((String)node.status.get("idc"), (String)node.status.get("ip"), (String)node.status.get("port"), (String)node.status.get("role"));
	}

	/**
	 * 将redis config get * 信息解析成map
	 * 
	 * @param info
	 * @return
	 */
	public static Map<String, String> extractRedisConfig(List<String> list) {
		Map<String, String> conf = null;
		if (list != null && list.size() != 0) {
			conf = new HashMap<String, String>();
			for (int i = 0; i < list.size() - 1; i = i + 2) {
				conf.put(list.get(i), list.get(i + 1));
			}
		}
		return conf;
	}

	/**
	 * 域名查IP组
	 * 
	 * @param url
	 * @return
	 */
	public static String[] domain2IP(String url) {
		try {
			InetAddress[] ip = InetAddress.getAllByName(url);
			String[] ips = new String[ip.length];
			for (int i = 0; i < ip.length; i++) {
				ips[i] = ip[i].toString().split("/")[1];
			}
			return ips;
		} catch (UnknownHostException e) {
			System.out.println("domain doesn't exist:" + url);
		}
		return null;
	}

	public static boolean isBlank(String str) {
		if (str == null || str.equals(""))
			return true;
		if (str.trim().equals(""))
			return true;
		return false;
	}

	/**
	 * IP地址转数字
	 * 
	 * @param ip
	 * @return
	 */
	public static int IP2Integer(String ip) {
		String[] iplist = ip.split("\\.");
		int sip1 = 0, sip2 = 0, sip3 = 0, sip4 = 0, cip = 0;
		sip1 = Integer.parseInt(iplist[0]);
		sip2 = Integer.parseInt(iplist[1]);
		sip3 = Integer.parseInt(iplist[2]);
		sip4 = Integer.parseInt(iplist[3]);
		if (sip1 < 128) {
			cip = sip1 * 256 * 256 * 256 + sip2 * 256 * 256 + sip3 * 256 + sip4;
		} else {
			cip = sip1 * 256 * 256 * 256 + sip2 * 256 * 256 + sip3 * 256 + sip4
					- (429496729 * 10) - 6;
		}
		cip = sip1 * 256 * 256 * 256 + sip2 * 256 * 256 + sip3 * 256 + sip4
				+ (429496729 * 10) + 6;

		return cip;
	}

	/**
	 * 获取多个字符串组成的path，例如/path1/path2/path3
	 * 
	 * @param paths
	 * @return
	 */
	public static String getAbsolutePath(String... paths) {
		StringBuilder builder = new StringBuilder();
		for (String path : paths) {
			if (path.endsWith("/")) {
				path = path.substring(0, path.length() - 1);
			}
			if (path.startsWith("/")) {
				builder.append(path);
			} else {
				builder.append("/" + path);
			}
		}
		return builder.toString();
	}

	private final static char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6' , '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
	public static String bytesToHex(byte[] bytes) {
		StringBuffer sb = new StringBuffer();
		int t;
		for (int i = 0; i < 16; i++) {// 16 == bytes.length;
			t = bytes[i];
			if (t < 0)
				t += 256;
			sb.append(hexDigits[(t >>> 4)]);
			sb.append(hexDigits[(t % 16)]);
		}
		return sb.toString();
	}
	
	public static String getMasterID(String name ,String id){
		byte[] data = String.valueOf(name+id).getBytes();
		byte[] md5;
		String masterID = null;
		try {
			md5 = MessageDigest.getInstance("MD5").digest(data);
			masterID = bytesToHex(md5);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return masterID;
	}
	
	public static String place2idc(String addrString){
		String idc=addrString;
		if(addrString.equals("yf")){
			idc="eos";
		}else if(addrString.equals("tc")){
			idc="hebe";
		}else if(addrString.equals("xd")){
			idc="mars";
		}else if(addrString.equals("ja")){
			idc="apollo";
		}else if(addrString.equals("tj")){
			idc="orion";
		}else if(addrString.equals("gz")){
			idc="atlas";
		}else if(addrString.equals("sh")){
			idc="aries";
		}
		return idc;
	}
	
	public static Map<String, String> extractRedisInfo(List<String> list){
		Map<String,String> info = new HashMap<String,String>();
		if(list != null && list.size()!=0){
			info = new HashMap <String, String>();
			for(int i=0; i <list.size()-1; i=i+1){
				List<String> itemlist= Arrays.asList(list.get(i).replaceAll("\r|\n", "").split(":",2));
				int itemnum=itemlist.size();
				if (itemnum == 2){
					String key=itemlist.get(0);
					String value=itemlist.get(1);
					info.put(key,value);
				}
			}
		}
		return info;
	}
	
	public static void main(String[] args) {
		domain2IP("rm9984.eos.grid.sina.com.cn");
	}
}
