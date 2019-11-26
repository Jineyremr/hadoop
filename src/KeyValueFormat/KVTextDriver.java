package KeyValueFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * kvformat驱动类
 *
 * @author mingxiang
 * @date 2019/11/26
 */
public class KVTextDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置
        Configuration conf = new Configuration();
        //设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        //配置jar路径
        job.setJarByClass(KVTextDriver.class);

        //配置mapper类和reducer类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        //配置mapper输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //配置最终kv输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //配置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("F:\\inout\\input\\kvformat"));
        //将inputformat类设置为KV
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path("F:\\inout\\output\\kvformat"));
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

}
