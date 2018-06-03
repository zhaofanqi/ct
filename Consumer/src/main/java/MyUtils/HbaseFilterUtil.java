package MyUtils;

import constant.Constant;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HbaseFilterUtil {
    public static Filter eqFilter(String buildTime_ts,String value,String...columnFamily ){
        //String[] cfs = new String[Constant.COLUMN_FAMILY_NUMS];
        String[] cfs=columnFamily.clone();
       return  new SingleColumnValueFilter(Bytes.toBytes(cfs[0]), Bytes.toBytes(buildTime_ts), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
    }
    public static Filter gteqFilter(String buildTime_ts,String value,String...columnFamily ){
        //String[] cfs = new String[Constant.COLUMN_FAMILY_NUMS];
        String[] cfs=columnFamily.clone();
       return  new SingleColumnValueFilter(Bytes.toBytes(cfs[0]), Bytes.toBytes(buildTime_ts), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(value));
    }
    public static Filter lseqFilter(String buildTime_ts,String value,String...columnFamily ){
        //String[] cfs = new String[Constant.COLUMN_FAMILY_NUMS];
        String[] cfs=columnFamily.clone();
       return  new SingleColumnValueFilter(Bytes.toBytes(cfs[0]), Bytes.toBytes(buildTime_ts), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(value));
    }
    public static Filter lsFilter(String buildTime_ts,String value,String...columnFamily ){
        //String[] cfs = new String[Constant.COLUMN_FAMILY_NUMS];
        String[] cfs=columnFamily.clone();
       return  new SingleColumnValueFilter(Bytes.toBytes(cfs[0]), Bytes.toBytes(buildTime_ts), CompareFilter.CompareOp.LESS, Bytes.toBytes(value));
    }

    public static FilterList addFilter(Filter... filters){
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        for (Filter filter : filters) {
            filterList.addFilter(filter);
        }
        return filterList ;
    }
    public static FilterList  orFilter(Filter...filters){
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Filter filter : filters) {
            filterList.addFilter(filter);
        }
        return filterList ;
    }

}
