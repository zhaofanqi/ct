import java.util.Arrays;
import java.util.List;

public class TempTest {

    public void test1(){
        List<String> list = Arrays.asList("cf", "info");
      // test(list.);
    }

    public void test(String... colomnFamily){
        System.out.println(colomnFamily.toString());
    }

    public static void main(String[] args) {
        TempTest tempTest = new TempTest();
        tempTest.test1();
    }
}
