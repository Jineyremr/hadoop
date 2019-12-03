package outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text,NullWritable> {

    FSDataOutputStream atOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        //获取文件系统
        FileSystem fs;
        try{
            fs = FileSystem.get(job.getConfiguration());

            //创建输出文件路径
            Path atPath = new Path("D:\\inout\\output\\outputformat\\at.log");
            Path otherPath = new Path("D:\\inout\\output\\outputformat\\other.log");
            //创建输出流
            atOut = fs.create(atPath);
            otherOut = fs.create(otherPath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断是否包含atguigu输出到不同的文件
        if(text.toString().contains("atguigu")){
            atOut.write(text.toString().getBytes());
        }else{
            otherOut.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atOut);
        IOUtils.closeStream(otherOut);
    }
}
