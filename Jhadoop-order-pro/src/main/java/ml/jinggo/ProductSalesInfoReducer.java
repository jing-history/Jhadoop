package ml.jinggo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangyj
 * @description
 * @create 2018-08-10 9:55
 **/
//                                                                      k4商品名称   v4（年份+金额）
public class ProductSalesInfoReducer  extends Reducer<IntWritable, Text, Text, Text> {
    @Override
    protected void reduce(IntWritable k3, Iterable<Text> v3, Context context)
            throws IOException, InterruptedException {
        String productName = "";

        //定义一个HashMap保存每一年的订单金额
        Map<Integer, Double> result = new HashMap<Integer, Double>();

        for (Text v:v3){
            String str = v.toString();
            //判断是否存在  name:
            int index = str.indexOf("name:");
            if(index >=0){
                productName = str.substring(5);
            }else {
                //words[2]+":"+words[6]
                //表示订单信息  订单的年份、金额    1998-01-10:1232.16
                int year = Integer.parseInt(str.substring(0, 4));
                double amount = Double.parseDouble(str.substring(str.lastIndexOf(":")+1));

                if(result.containsKey(year)){
                    //如果已经有订单，累计
                    result.put(year, result.get(year) + amount);
                }else{
                    result.put(year, amount);
                }
            }
        }
        //输出
        context.write(new Text(productName), new Text(result.toString()));
    }
}
