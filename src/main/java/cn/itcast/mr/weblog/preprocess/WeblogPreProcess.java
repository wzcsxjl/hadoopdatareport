package cn.itcast.mr.weblog.preprocess;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itcast.mr.weblog.bean.WebLogBean;

/**
 * 处理原始日志，过滤出真实请求数据
 * 日志数据预处理：数据清洗、日期格式转换、缺失字段填充默认值、字段添加标记valid和invalid
 * 
 * @author wzcsx
 *
 */
public class WeblogPreProcess {

	// Mapreduce程序执行这一类
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WeblogPreProcess.class);
		job.setMapperClass(WeblogPreProcessMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 此次案例测试数据量不是非常大，所以启用本地默认
		// （实际情况会对HDFS上存储的文件进行处理）
		FileInputFormat.setInputPaths(job, new Path("D:/weblog/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/weblog/output"));
		// 将ReduceTask数设置为0，不需要Reduce阶段
		job.setNumReduceTasks(0);
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}

	// Mapreduce程序Map阶段
	public static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		// 用来存储网站URL分类数据
		Set<String> pages = new HashSet<>();
		Text k = new Text();
		NullWritable v = NullWritable.get();

		/**
		 * setup()仅在执行map()方法之前被调用一次，通常用于初始化加载数据，存储到MapTask内存中
		 * 设置初始化方法，用来表示用户请求的是合法资源
		 * 加载网站需要分析的URL分类数据，存储到MapTask的内存中，用来对日志数据进行过滤
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			pages.add("/about/");
			pages.add("/black-ip-list/");
			pages.add("/cassandra-clustor/");
			pages.add("/finance-rhive-repurchase/");
			pages.add("/hadoop-family-roadmap/");
			pages.add("/hadoop-hive-intro/");
			pages.add("/hadoop-zookeeper-intro/");
			pages.add("/hadoop-mahout-roadmap/");
		}

		/**
		 * 重写map()方法，对每行记录重新解析转换并输出
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 获取一行数据
			String line = value.toString();
			// 调用解析类WebLogParser解析日志数据，最后封装为WebLogBean对象
			WebLogBean webLogBean = WebLogParser.parser(line);
			if (webLogBean != null) {
				// 过滤js/图片/css等静态资源
				WebLogParser.filtStaticResource(webLogBean, pages);
				k.set(webLogBean.toString());
				context.write(k, v);
			}
		}
	}

}
