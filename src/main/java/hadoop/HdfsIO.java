package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsIO {

    public static void main(String[] args) throws Exception {
        System.out.println("hello hadoop");
        Configuration conf = new Configuration();
        conf.addResource(new Path("C:\\hdp\\hadoop-2.7.3\\etc\\hadoop\\core-site.xml"));// core-site.xml
        conf.addResource(new Path("C:\\hdp\\hadoop-2.7.3\\etc\\hadoop\\hdfs-site.xml"));// hdfs-site.xml
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("error in connecting to HDFS ");
        }

        FSDataOutputStream outputStream = fs.create(new Path("/data/csvfiles/coco.txt"));
        outputStream.writeBytes("kamal");

    }
}
