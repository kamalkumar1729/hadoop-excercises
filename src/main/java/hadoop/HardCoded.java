package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HardCoded {
    public static void main(String[] args) throws Exception {

        System.out.println("hello hadoop");
        Configuration conf = new Configuration();

        conf.set("fs.defaultFs","hdfs://localhost:9000");
        System.out.println(conf.get("fs.defaultFS"));

        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
           // e.printStackTrace();
            System.out.println("error in connecting to HDFS ");
        }
       // System.out.println(fs.exists(new Path("/data")));
        System.out.println(fs.exists(new Path("/")));
        System.out.println(fs.mkdirs(new Path("/data")));
        FileStatus fileStatus = fs.getFileStatus(new Path("/data"));
        System.out.println(fileStatus.isFile());

    }
}
