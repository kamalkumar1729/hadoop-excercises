package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBaseConnect {

    public static void main(String[] args) throws Exception{

        /* Deprecated */
       // Configuration config = new HBaseConfiguration();

        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path("C:\\hdp\\hbase-1.1.2\\conf\\hbase-site.xml"));

        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

       // Table table = conn.getTable();

        if (conn != null) {

            System.out.println("HBase connection is acquired.");
            TableName tableName = TableName.valueOf("emp");

            conn.getTable(TableName.valueOf("hello tables"));

            Table table = conn.getTable(tableName);
            Scan scan = new Scan();

            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                System.out.println(result.toString());
            }
        }
    }
}
