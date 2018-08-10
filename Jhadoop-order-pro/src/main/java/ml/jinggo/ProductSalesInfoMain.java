package ml.jinggo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author wangyj
 * @description
 * @create 2018-08-10 10:17
 **/
public class ProductSalesInfoMain {

    public static void main(String[] args) throws Exception {
        //1、创建Job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ProductSalesInfoMain.class);

        //2、指定任务的Mapper和输出的类型
        job.setMapperClass(ProductSalesInfoMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //3、指定任务的Reducer和输出的类型
        job.setReducerClass(ProductSalesInfoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //4、任务的输入和输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、执行
        job.waitForCompletion(true);
    }
}
