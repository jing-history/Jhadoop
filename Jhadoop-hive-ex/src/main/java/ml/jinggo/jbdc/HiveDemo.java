package ml.jinggo.jbdc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author wangyj
 * @description
 * @create 2018-08-03 11:19
 **/
public class HiveDemo {

    public static void main(String[] args) {
        //查询员工信息
        String sql = "select * from emp1";

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        try{
            conn = JDBCUtils.getConnection();

            //得到sql 的运行环境
            st = conn.createStatement();

            //运行sql
            rs = st.executeQuery(sql);

            while(rs.next()) {
                //姓名和薪水
                //注意：好像不能通过列的索引号获取
                String ename = rs.getString("ename");
                double sal = rs.getDouble("sal");
                System.out.println(ename + "\t" + sal);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            JDBCUtils.release(conn,st,rs);
        }
    }
}
