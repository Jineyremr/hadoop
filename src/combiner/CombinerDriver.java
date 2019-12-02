package combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordcount.WordcountMapper;
import wordcount.WordcountReducer;

import java.io.IOException;

public class CombinerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar加载路径
        job.setJarByClass(CombinerDriver.class);
        //设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        //设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // ** 设置combiner类
        job.setCombinerClass(WordCountCombiner.class);

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\inout\\input\\wordcount"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\inout\\output\\combiner"));
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
