package groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    //订单id
    private int order_id;
    //订单价格
    private double price;

    public OrderBean() {
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int compareTo(OrderBean o) {
        int result = 0;
        if(this.order_id>o.order_id){
            result = 1;
        }
        if(this.order_id<o.order_id){
            result = -1;
        }
        if(this.order_id==o.order_id){
            //二次排序，价格倒叙排序
            result = this.price >o.price ? -1 : 1;
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.order_id);
        dataOutput.writeDouble(this.price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }
}
