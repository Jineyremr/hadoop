package etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Classname LogProDriver
 * @Description 增强版数据过滤driver
 * @Date 2019/12/3 17:01
 * @Author chengmx
 */
public class LogProDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(LogProDriver.class);

        // 3 关联map
        job.setMapperClass(LogProMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\inout\\input\\etl"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\inout\\output\\etlpro"));

        // 6 提交
        job.waitForCompletion(true);

    }
}
