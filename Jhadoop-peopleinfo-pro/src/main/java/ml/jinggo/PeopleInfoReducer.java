package ml.jinggo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyj
 * @description
 * @create 2018-07-16 15:02
 **/
//                                                               性别        统计值
public class PeopleInfoReducer extends Reducer<Text, PeopleInfo, Text, IntWritable> {

    @Override
    protected void reduce(Text key3, Iterable<PeopleInfo> value3,Context context) throws IOException, InterruptedException {
        // 定义几个变量保存结果
        int totalNumber = 0; //总人数
        int highest = 0;//最高身高
        int lowest = 10000 ; //最低值  注意初始值

        for(PeopleInfo p:value3){
            //总人数+1
            totalNumber ++;

            //得到最高值和最低值
            if(p.getHeight() > highest){
                highest = p.getHeight();
            }

            if(p.getHeight() < lowest){
                lowest = p.getHeight();
            }
        }

        //输出
        context.write(new Text("Total: "), new IntWritable(totalNumber));
        context.write(new Text("highest: "), new IntWritable(highest));
        context.write(new Text("lowest: "), new IntWritable(lowest));
    }
}
