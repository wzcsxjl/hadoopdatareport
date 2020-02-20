package cn.itcast.mr.weblog.preprocess;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

import cn.itcast.mr.weblog.bean.WebLogBean;

public class WebLogParser {

	// 定义时间格式
	public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	/**
	 * 根据采集的数据字段信息进行解析封装
	 */
	public static WebLogBean parser(String line) {
		WebLogBean webLogBean = new WebLogBean();
		// 把一行数据以空格字符切割并存入数组arr中
		String[] arr = line.split(" ");
		// 如果数组长度小于等于11，说明这条数据不完整，因此可以忽略这条数据
		if (arr.length > 11) {
			// 满足条件的数据逐个赋值给webLogBean对象
			// 58.215.204.118
			webLogBean.setRemote_addr(arr[0]);
			// -
			webLogBean.setRemote_user(arr[1]);
			// 18/Sep/2013:06:51:35
			String time_local = formatDate(arr[3].substring(1));
			if (null == time_local || "".equals(time_local))
				time_local = "-invalid_time-";
			webLogBean.setTime_local(time_local);
			// /nodejs-socketio-chat/
			webLogBean.setRequest(arr[6]);
			// 200
			webLogBean.setStatus(arr[8]);
			// 10818
			webLogBean.setBody_bytes_sent(arr[9]);
			// "http://blog.fens.me/nodejs-socketio-chat/"
			webLogBean.setHttp_referer(arr[10]);
			// 如果useragent元素较多，拼接useragent
			if (arr.length > 12) {
				StringBuilder sb = new StringBuilder();
				for (int i = 11; i < arr.length; i++) {
					sb.append(arr[i]);
				}
				webLogBean.setHttp_user_agent(sb.toString());
			} else {
				webLogBean.setHttp_user_agent(arr[11]);
			}
			// 大于400，HTTP错误
			if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
				webLogBean.setValid(false);
			}
			if ("-invalid_time-".equals(webLogBean.getTime_local())) {
				webLogBean.setValid(false);
			}
		} else {
			webLogBean = null;
		}
		return webLogBean;
	}

	/**
	 * 对请求路径资源是否合法进行标记
	 */
	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		if (!pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}

	/**
	 * 格式化时间方法
	 */
	public static String formatDate(String time_local) {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			return null;
		}
	}

}
