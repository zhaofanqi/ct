package constant;

import MyUtils.KafkaUtils;

import java.util.Properties;

public class Constant {

    public static final Integer SERVER_REGIONS_RATION=2;
    public static final Integer REGIONS= Integer.valueOf(KafkaUtils.properties.getProperty("server.num"))*SERVER_REGIONS_RATION;
    public static final Integer COLUMN_FAMILY_NUMS=5;
    public static final String FLAG="1";
}
