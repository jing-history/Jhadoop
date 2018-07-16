package ml.jinggo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author wangyj
 * @description
 * @create 2018-07-16 14:18
 **/
public class PeopleInfoMain {

    public static void main(String[] args) throws IOException {
        //1、创建Job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PeopleInfoMain.class);

        //2、指定任务的Mapper和输出的类型

    }
}
