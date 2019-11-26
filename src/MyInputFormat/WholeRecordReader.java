package MyInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    private Configuration configuration;
    private FileSplit split;
    private boolean isProgress = true;
    BytesWritable v = new BytesWritable();
    Text k = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isProgress){
            //定义缓存区
            byte[] contemts = new byte[(int) split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;
            try{
                Path path = split.getPath();
                FileSystem fileSystem = path.getFileSystem(configuration);
                fis = fs.open(path);
                IOUtils.readFully(fis,contemts,0,contemts.length);
                v.set(contemts,0,contemts.length);
                String name = split.getPath().toString();
                k.set(name);
            }catch (Exception e){
            }finally {
                IOUtils.closeStream(fis);
            }

            isProgress = false;
            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
