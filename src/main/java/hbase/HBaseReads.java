package hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseReads {

    // TODO :=> write & read are important.
    //  Other concepts/operations can be ignored for now, Okay.

    public static void main(String[] args) throws Exception {
        HBaseReadsUsingGets();
        HBaseReadsUsingScan();
    }

    private static void HBaseReadsUsingScan() throws Exception {
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes("115"));

        Table empTable = HBaseUtils.getTable("emp");
        ResultScanner scanner = empTable.getScanner(scan);

        /* ResultScanner => iterator */
        for (Result result : scanner) {
            System.out.println(result.toString());
        }
    }


    private static void HBaseReadsUsingGets() throws Exception {
        /* Get for single record; Get(rowkey); */

        /* Command => get 'tableName', 'rowkey' */
        /* Command => get 'employee', '55019688' */
        Get get = new Get(Bytes.toBytes("102"));
        Get get2 = new Get(Bytes.toBytes("108"));

        List<Get> getList = new ArrayList<Get>();
        getList.add(get); getList.add(get2);

        Table empTable = HBaseUtils.getTable("emp");

        /* Result get(Get get) throws IOException; */
        /* Result[] get(List<Get> getList) throws IOException; */
        Result[] resultList = empTable.get(getList);

        for (Result result : resultList) {
            byte[] bytesArr = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(bytesArr));
        }
    }
}
