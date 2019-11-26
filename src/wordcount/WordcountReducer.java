package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计单词数
 *
 * @author chengmx
 * @date 2019/11/26
 */
public class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    int sum;
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        //累加
        for(IntWritable count :values){
            sum+=count.get();
        }
        //输出
        v.set(sum);
        context.write(key,v);
    }
}
