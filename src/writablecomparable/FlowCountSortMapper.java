package writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phoneNum = split[1];
        long upFlow = Long.parseLong(split[3]);
        long downFlow = Long.parseLong(split[4]);
        k.set(upFlow,downFlow);
        v.set(phoneNum);
        context.write(k,v);
    }
}
