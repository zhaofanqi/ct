import MyUtils.HbaseFilterUtil;
import MyUtils.HbaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class HbaseFilterTest {
    /*
    需求：扫描整张表获取指定时间内，
     */
    public static void main(String[] args) throws ParseException {
        try {
            Table calllog = HbaseUtil.connection.getTable(TableName.valueOf("ct:calllog"));
            Scan scan = new Scan();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String startTime = "2019-03-01";
            String endTime = "2019-05-01";
            //筛选条件的开始时间
            String buildTime_ts = "buildTime_ts";
            String value1 = String.valueOf(sdf.parse(startTime).getTime());
            String columnFamilys = "f1";
            //筛选条件的结束时间
            String value2 = String.valueOf(sdf.parse(endTime).getTime());

            Filter filter1 = HbaseFilterUtil.gteqFilter(buildTime_ts, value1, columnFamilys);
            Filter filter2 = HbaseFilterUtil.lseqFilter(buildTime_ts, value2, columnFamilys);
            //List<Filter> filters = HbaseFilterUtil.addFilter(filter1, filter2);
            scan.setFilter(filter1);
            scan.setFilter(filter2);
            ResultScanner scanner = calllog.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    CellUtil.cloneFamily(cell);
                    System.out.println("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + " Column:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + " Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                            + " Row :" + Bytes.toString(CellUtil.cloneRow(cell)));
                    //byte[] bytes = CellUtil.cloneRow(cell);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    需求：扫描整张表获取指定时间内，指定用户的通话记录
     */
    @Test
    public void test() {
        try {
            Table calllog = HbaseUtil.connection.getTable(TableName.valueOf("ct:calllog"));
            Scan scan = new Scan();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String startTime = "2019-03-01";
            String endTime = "2019-05-01";
            //筛选条件的开始时间
            String buildTime_ts = "buildTime_ts";
            String value1 = String.valueOf(sdf.parse(startTime).getTime());
            String columnFamilys = "f1";

            //筛选条件的结束时间
            String value2 = String.valueOf(sdf.parse(endTime).getTime());
            String callNumber="17472538197";
            String call1="call1";
            String call2="call2";


            Filter filter1 = HbaseFilterUtil.gteqFilter(buildTime_ts, value1, columnFamilys);
            Filter filter2 = HbaseFilterUtil.lseqFilter(buildTime_ts, value2, columnFamilys);
            FilterList filterList1 = HbaseFilterUtil.addFilter(filter1, filter2);

            Filter filter3 = HbaseFilterUtil.eqFilter(call1, callNumber, columnFamilys);
            Filter filter4 = HbaseFilterUtil.eqFilter(call2, callNumber, columnFamilys);
            FilterList filterList2 = HbaseFilterUtil.orFilter(filter3, filter4);

            scan.setFilter(filterList1);
            scan.setFilter(filterList2);

            ResultScanner scanner = calllog.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                   // Bytes.toString(CellUtil.cloneRow(cell)).
                    System.out.println("ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + " Column:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + " Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                            + " Row :" + Bytes.toString(CellUtil.cloneRow(cell)));
                    //byte[] bytes = CellUtil.cloneRow(cell);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
