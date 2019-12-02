package partition;

import flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String preNum = text.toString().substring(0, 3);
        int partition = 4;
        if("132".equals(preNum)){
            partition = 0;
        }
        if("133".equals(preNum)){
            partition = 1;
        }
        if("134".equals(preNum)){
            partition = 2;
        }
        if("136".equals(preNum)){
            partition = 3;
        }


        return partition;
    }
}
