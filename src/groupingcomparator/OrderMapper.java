package groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        int order_id = Integer.parseInt(split[0]);
        double price = Double.parseDouble(split[2]);
        k.setOrder_id(order_id);
        k.setPrice(price);
        context.write(k,NullWritable.get());
    }
}
