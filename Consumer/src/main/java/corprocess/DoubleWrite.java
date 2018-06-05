package corprocess;


import MyUtils.HbaseUtil;
import MyUtils.KafkaUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class DoubleWrite extends BaseRegionObserver {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //rowkey 格式为：recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+flag+"_"+duration;
    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
                        final Put put, final WALEdit edit, final Durability durability)  {
        //此时还是需要往表中写数据
        String befTable = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String curTable = KafkaUtils.properties.getProperty("tableName");
        if (!curTable.equals(befTable)) return;
        //这表示的是获取rowKey
        byte[] row = put.getRow();
        System.out.println("this is row-----------------------------see "+Bytes.toString(row));
        String[] record= Bytes.toString(row).split("_");
        //  String[] record = row.toString().split("_");

        String flag = record[4];
        if ("0".equals(flag))
            return;
        flag = "0";
        String caller = record[1];
        String buildTime = record[2];
        String buildTime_ts = null;
        try {
            buildTime_ts = sdf.parse(buildTime).getTime() + "";
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
        String callee = record[3];
        String duration = record[5];
        String rowKeyWithFlag = HbaseUtil.getRowKeyWithFlag(callee, caller, buildTime, duration, "0");
        Put newPut = new Put(Bytes.toBytes(rowKeyWithFlag));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime_ts"), Bytes.toBytes(buildTime_ts));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        try {
            Table table = HbaseUtil.connection.getTable(TableName.valueOf(curTable));
            table.put(newPut);
            table.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

    }
}
