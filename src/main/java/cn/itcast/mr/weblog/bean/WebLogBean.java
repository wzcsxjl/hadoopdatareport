package cn.itcast.mr.weblog.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 对接外部数据的层，表结构定义最好跟外部数据源保持一致；同时实现序列化，方便网络数据传输
 * 术语：贴源表
 * 
 * @author wzcsx
 *
 */
public class WebLogBean implements Writable {

	private boolean valid = true; // 标记数据是否合法
	private String remote_addr; // 访客IP地址
	private String remote_user; // 记录访客用户信息，忽略属性“-”
	private String time_local; // 记录访问时间与时区
	private String request; // 记录请求的URL
	private String status; // 记录请求状态；成功是200
	private String body_bytes_sent; //记录发送给客户端文件主体内容大小
	private String http_referer; // 记录从哪个页面链接访问过来的
	private String http_user_agent; // 记录客户浏览器的相关信息

	/**
	 * 设置WebLogBean（属性值）进行字段数据封装
	 * 
	 * @param valid
	 * @param remote_addr
	 * @param remote_user
	 * @param time_local
	 * @param request
	 * @param status
	 * @param body_bytes_sent
	 * @param http_referer
	 * @param http_user_agent
	 */
	public void setBean(boolean valid, String remote_addr, String remote_user, String time_local, String request,
			String status, String body_bytes_sent, String http_referer, String http_user_agent) {
		this.valid = valid;
		this.remote_addr = remote_addr;
		this.remote_user = remote_user;
		this.time_local = time_local;
		this.request = request;
		this.status = status;
		this.body_bytes_sent = body_bytes_sent;
		this.http_referer = http_referer;
		this.http_user_agent = http_user_agent;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public String getTime_local() {
		return time_local;
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	/**
	 * 重写toString()方法，使用Hive默认分隔符进行分隔，为后期导入Hive表提供便利
	 * @return
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.valid);
		sb.append("\001").append(this.getRemote_addr());
		sb.append("\001").append(this.getRemote_user());
		sb.append("\001").append(this.getTime_local());
		sb.append("\001").append(this.getRequest());
		sb.append("\001").append(this.getStatus());
		sb.append("\001").append(this.getBody_bytes_sent());
		sb.append("\001").append(this.getHttp_referer());
		sb.append("\001").append(this.getHttp_user_agent());
		return sb.toString();
	}

	/**
	 * 序列化方法
	 * 
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.valid = in.readBoolean();
		this.remote_addr = in.readUTF();
		this.remote_user = in.readUTF();
		this.time_local = in.readUTF();
		this.request = in.readUTF();
		this.status = in.readUTF();
		this.body_bytes_sent = in.readUTF();
		this.http_referer = in.readUTF();
		this.http_user_agent = in.readUTF();
	}

	/**
	 * 反序列化方法（与序列化方法顺序保持一致）
	 * 
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.valid);
		out.writeUTF(null == remote_addr ? "" : remote_addr);
		out.writeUTF(null == remote_user ? "" : remote_user);
		out.writeUTF(null == time_local ? "" : time_local);
		out.writeUTF(null == request ? "" : request);
		out.writeUTF(null == status ? "" : status);
		out.writeUTF(null == body_bytes_sent ? "" : body_bytes_sent);
		out.writeUTF(null == http_referer ? "" : http_referer);
		out.writeUTF(null == http_user_agent ? "" : http_user_agent);
	}

}
