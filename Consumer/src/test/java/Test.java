import MyUtils.HbaseUtil;
import MyUtils.KafkaUtils;
import com.atguigu.HbaseConsumer;
import constant.Constant;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.TreeSet;

public class Test {
    private HbaseConsumer hbaseConsumer;
//    @org.junit.Test
//    public  void test1(){
//         hbaseConsumer = new HbaseConsumer();
//        try {
//            hbaseConsumer.kafkaConsumer();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
public static void main(String[] args) {
    int regions = 6;
    byte[][] splitKeys = new byte[regions][];
    //制作分区键
    //使用treeSet是为了排序去重，同时treeSet是无序的，无法按顺序放入byte[][]
    TreeSet<byte[]> treeByte = new TreeSet<>(Bytes.BYTES_COMPARATOR);

    //对数据分区键前2位格式化，不足的用0补足
    DecimalFormat df = new DecimalFormat("00");

    for (int i = 0; i < regions; i++) {
        treeByte.add(Bytes.toBytes(df.format(i) + "|"));
    }
    int i = 0;
    for (byte[] bytes : treeByte) {
        splitKeys[i] = bytes;
        i++;
    }
}
}
