package ml.jinggo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangyj
 * @description
 * @create 2018-07-16 14:20
 **/
public class PeopleInfoMapper extends Mapper<LongWritable, Text, Text, PeopleInfo> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        String data = value1.toString();
        String[] words = data.split(" ");

        //生成对象
        PeopleInfo info = new PeopleInfo();
        info.setPeopleID(Integer.parseInt(words[0]));
        info.setGender(words[1]);
        info.setHeight(Integer.parseInt(words[2]));

        context.write(new Text(info.getGender()), info);
    }

}

