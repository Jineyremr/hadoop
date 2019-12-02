package flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author mingxiang
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean fb = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        String phoneNum = fileds[1];
        long phoneUpFlow = Long.parseLong(fileds[fileds.length-3]);
        long phoneDownFlow = Long.parseLong(fileds[fileds.length-2]);
        long phneSumFlow = phoneUpFlow + phoneDownFlow;
        k.set(phoneNum);
        fb.set(phoneUpFlow,phoneDownFlow);
        context.write(k,fb);
    }
}
