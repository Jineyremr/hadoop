package reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();
        //准备bean对象
        TableBean pdBean = new TableBean();
        for(TableBean tb : values){
            if("order".equals(tb.getFlag())){
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean,tb);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,tb);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //表的合并
        for(TableBean tb : orderBeans){
            tb.setPname(pdBean.getPname());
            context.write(tb,NullWritable.get());
        }

    }
}
