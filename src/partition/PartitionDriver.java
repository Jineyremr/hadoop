package partition;

import flowsum.FlowBean;
import flowsum.FlowCountMapper;
import flowsum.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionDriver {

    public static void main(String[] arg0) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PartitionDriver.class);
        //配置mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        //配置mapper输出和最终输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // **设置自定义分区和分区数量
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //设置输入输出路径
        FileInputFormat.addInputPath(job,new Path("D:\\inout\\input\\partition"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\inout\\output\\partition"));
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

}
