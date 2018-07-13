package ml.jinggo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author wangyj
 * @description
 * @create 2018-07-07 14:40
 **/
public class SogouMain {

    public static void main(String[] args) throws IOException {
        //1、创建Job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SogouMain.class);

        //2、指定任务的Mapper和输出的类型
        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
    }
}
