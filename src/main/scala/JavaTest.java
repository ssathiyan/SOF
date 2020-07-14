/**
 * Created by STYN on 27-06-2020
 */
class JavaTest1 {
    String s1 = "x";
    public JavaTest1(){
        myMethod();
    }
    void myMethod()
    {
        System.out.println(s1);
    }
}

public class JavaTest extends JavaTest1{
    String s2 = "y";
    void myMethod()
    {
        //System.out.println(s1);
        System.out.println(s1 + ":" + s2);
    }

    public static void main(String[] args)
    {
        JavaTest j = new JavaTest();
        j.myMethod();
    }
}
