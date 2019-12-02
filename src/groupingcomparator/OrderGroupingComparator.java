package groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {


    public OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result = 0;
        if(aBean.getOrder_id()>bBean.getOrder_id()){
            result = 1;
        }
        if(aBean.getOrder_id()<bBean.getOrder_id()){
            result = -1;
        }
        return result;
    }
}
