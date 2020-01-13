package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseAdmin {

    public static void main(String[] args) throws Exception {
        Connection conn = HBaseUtils.getConnection();
        /* admin operations in HBase */

        Admin admin = conn.getAdmin();

       //  System.out.println(admin.listTableNames().length);

        for (TableName tableName : admin.listTableNames()) {
            System.out.println(tableName.toString());
        }


    }
}
