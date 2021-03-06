package etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname LogMapper
 * @Description 数据清理mapper
 * @Date 2019/12/3 16:46
 * @Author chengmx
 */
public class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean result = parseLog(line,context);
        //日志不合法退出
        if(!result){
            return;
        }
        k.set(line);
        context.write(k,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] split = line.split(" ");
        if(split.length>11){
            context.getCounter("map","true").increment(1);
            return true;
        }else{
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
