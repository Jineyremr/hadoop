package reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    String name;
    TableBean bean = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取输入文件切片
        FileSplit split = (FileSplit) context.getInputSplit();
        //获取输入文件名称
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        if(name.startsWith("order")){
            //订单表处理
            //封装bean对象
            bean.setOrder_id(split[0]);
            bean.setP_id(split[1]);
            bean.setAmount(Integer.parseInt(split[2]));
            bean.setPname("");
            bean.setFlag("order");
            k.set(split[1]);
        }else{
            //商品表处理
            //封装表数据
            bean.setOrder_id("");
            bean.setP_id(split[0]);
            bean.setAmount(0);
            bean.setPname(split[1]);
            bean.setFlag("pd");
            k.set(split[0]);
        }

        context.write(k,bean);
    }
}
