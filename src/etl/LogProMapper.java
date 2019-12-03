package etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname LogProMapper
 * @Description 增强版数据过滤mapper
 * @Date 2019/12/3 17:01
 * @Author chengmx
 */
public class LogProMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        LogBean  bean = parseLog(line,context);
        if(!bean.isValid()){
            return;
        }
        k.set(bean.toString());
        context.write(k,NullWritable.get());
    }

    private LogBean parseLog(String line, Context context) {
        LogBean logBean = new LogBean();
        String[] split = line.split(" ");
        if(split.length>11){
            logBean.setRemote_addr(split[0]);
            logBean.setRemote_user(split[1]);
            logBean.setTime_local(split[3].substring(1));
            logBean.setRequest(split[6]);
            logBean.setStatus(split[8]);
            logBean.setBody_bytes_sent(split[9]);
            logBean.setHttp_referer(split[10]);

            if(split.length>12){
                logBean.setHttp_user_agent(split[11] + " "+ split[12]);
            }else {
                logBean.setHttp_user_agent(split[11]);
            }

            //大于400，http错误
            if(Integer.parseInt(logBean.getStatus())>=400){
                logBean.setValid(false);
            }
        }else{
            logBean.setValid(true);
        }

        return logBean;
    }
}
