package hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseWrites {

    public static void main(String[] args) throws Exception {
        // TODO :=> write & read are important.
        //  Other concepts/operations can be ignored for now, Okay.

        /* Put(rowkey(string)) ,
           addColumn(Bytes(Family),Bytes(ColumnName),Bytes(Value)) */

        /* Command => put 'tableName', 'rowkey', 'family:column', 'Value' */
        /* Command => put 'employee', '55019688', 'personal:name','Steve'; */

        Put put = new Put(Bytes.toBytes("1150"),1234567890);
        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("KAMAL"));

        Put put2 = new Put(Bytes.toBytes("116"));
        put2.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("EC"), Bytes.toBytes("55019688"));

        List<Put> putList = new ArrayList<Put>();
        putList.add(put);
        putList.add(put2);

        /* Get table from Connection() */
        Table table = HBaseUtils.getTable("emp");


        /* void put(Put put) throws IOException; */
        /* void put(List<Put> putList) throws IOException; */
        table.put(putList);
        table.put(put);
        table.close();

        System.out.println("data inserted");
    }
}