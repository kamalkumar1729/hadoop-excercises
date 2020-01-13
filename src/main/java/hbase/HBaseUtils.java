package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

class HBaseUtils {

    private static Configuration configuration;

    private static void initializeConfiguration() {
        configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("C:\\hdp\\hbase-1.1.2\\conf\\hbase-site.xml"));
    }

    static Connection getConnection() throws IOException {
        if (configuration == null) {
            initializeConfiguration();
        }
        return ConnectionFactory.createConnection(configuration);
    }

    static Table getTable(String tableName) throws Exception {
        if (tableName == null || tableName.equalsIgnoreCase("")) {
            throw new IllegalArgumentException("Table name can not be null or empty");
        }
        return getConnection().getTable(TableName.valueOf(tableName));
    }
}