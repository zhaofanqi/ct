package MyUtils;

import constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.TreeSet;

public class HbaseUtil {
    private static Configuration configuration;
    public  static Connection connection = null;
    private static Admin admin = null;
    private static DecimalFormat df=new DecimalFormat("00");

    //初始化命名空间
    static {
        configuration = HBaseConfiguration.create();
        try {
            //获取连接
            connection = ConnectionFactory.createConnection(configuration);
            //获取admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void initNameSpace(String nameSpace) throws IOException {

       try{
           NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).addConfiguration("create_time", String.valueOf(System.currentTimeMillis())).build();
           admin.createNamespace(namespaceDescriptor);
       }catch (Exception e){
           System.out.println("namespace "+nameSpace+ "already exist_zfq");
       }


      //  close(connection, admin);
    }

    /*
    创建表
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("the table " + tableName + "is already exist!!");
            return;
        }
        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族
        for (String cf : columnFamily) {
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        //创建表之前需要注册协处理器
      tableDescriptor.addCoprocessor("corprocess.DoubleWrite");
        //创建表（带分区健）
        admin.createTable(tableDescriptor, getSplitKeys());
       // close(connection, admin);
    }

    public static void close(Connection connection, Admin admin, Table... table) {
        try {
            if (connection != null) connection.close();
            if (admin != null) admin.close();
            if (table != null) {
                for (Table table1 : table) {
                    table1.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建分区键
    public static byte[][] getSplitKeys() {
        //获取分区键的个数被全局申明
       // int regions = Integer.valueOf(KafkaUtils.properties.getProperty("server.num")) * Constant.SERVER_REGIONS_RATION;
        byte[][] splitKeys = new byte[Constant.REGIONS][];
        //制作分区键
        //使用treeSet是为了排序去重，同时treeSet是无序的，无法按顺序放入byte[][]
        //TODO 为什么要有Bytes.BYTES_COMPARATOR？
        TreeSet<byte[]> treeByte = new TreeSet<>(Bytes.BYTES_COMPARATOR);

        //对数据分区键前2位格式化，不足的用0补足
        //DecimalFormat df = new DecimalFormat("00");

        for (int i = 0; i < Constant.REGIONS; i++) {
            treeByte.add(Bytes.toBytes(df.format(i) + "|"));
        }
        int i = 0;
        for (byte[] bytes : treeByte) {
            splitKeys[i] = bytes;
            i++;
        }
        return splitKeys;
    }

    //制造rowkey  15596505995,17519874292,2017-03-11 00:30:19,0652
    public static  String  getRowKey(String caller,String callee,String buildTime,String duration){
        //rowkey:分区位_caller_bulideTime_callee_duration
        //获取分区位
        String recordHash = getRecordHash(caller, buildTime);
        return  recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+duration;
    }
    public static  String  getRowKeyWithFlag(String caller,String callee,String buildTime,String duration,String flag){
        //rowkey:分区位_caller_bulideTime_callee_flag_duration
        //获取分区位
        String recordHash = getRecordHash(caller, buildTime);
        System.out.println("this  is getRowKeyWithFlag "+recordHash + "_" + caller + "_" + buildTime + "_" + callee + "_" + flag + "_" + duration);
        return  recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+flag+"_"+duration;
    }
    /*
    目的：让电话随机分布在各个区，制造分区的前三为电话号码的后四位和通话建立时间的前六位异或得到
     */
    public  static String getRecordHash(String caller,String buildTime) {
        int regions;
        String call = caller.substring(caller.length() - 4);
        String build = buildTime.replace("-", "").substring(0, 6);
        int t= (Integer.valueOf(call) ^ Integer.valueOf(build)) % Constant.REGIONS;
        return df.format(t);
    }
}
