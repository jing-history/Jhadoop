package ml.jinggo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyj
 * @description
 * @create 2018-07-07 14:41
 **/
//                                       k1偏移量                v1数据    k2日志数据     v2
public class SogouMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        //数据：20111230000005	57375476989eea12893c0c3811607bcf	奇艺高清	1	1	http://www.qiyi.com/
        String log = value1.toString();
        String[] words = log.split("\t");

        //数据的清洗
        if(words.length != 6) return;

        try{
            //找到URL排名第一、用户点击顺序第二的日志
            if(Integer.parseInt(words[3]) == 1 && Integer.parseInt(words[4]) == 2){
                //输出
                context.write(value1, NullWritable.get());
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
