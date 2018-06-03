package MyUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaUtils {
    public static Properties properties=null;
     static  {
        InputStream sra = ClassLoader.getSystemResourceAsStream("kafka.properties");
        properties=new Properties();
         try {
             properties.load(sra);
         } catch (IOException e) {
             e.printStackTrace();
         }
         System.out.println(properties.getProperty("bootstrap.servers"));
    }


}
