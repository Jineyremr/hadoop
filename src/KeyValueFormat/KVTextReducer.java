package KeyValueFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 验证kvformat
 *
 * @author mingxiang
 * @date 2019/11/26
 */
public class KVTextReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    int sum;
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for(IntWritable value : values){
            sum+=value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
