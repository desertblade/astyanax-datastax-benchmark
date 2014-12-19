
/**
 * beta astyanax branch which uses java driver underneath. The driver is much slower than the thrift driver.
 * https://github.com/Netflix/astyanax/issues/471
*/


public class Benchmark {

    public static void main(String[] args) {

        DatastaxImpl datastax = new DatastaxImpl();
        AstyanaxImpl astyanax = new AstyanaxImpl();

        try {

            long anStartTime = System.currentTimeMillis();
            for(int i=1; i<10000; i++)
            {
                String name = astyanax.findById("me");
            }
            long anEstimatedTime = System.currentTimeMillis() - anStartTime;

            long dsStartTime = System.currentTimeMillis();
            for(int i=1; i<10000; i++)
            {
                String name = "Datastax for 'me': " + datastax.findById("me");
            }
            long dsEstimatedTime = System.currentTimeMillis() - dsStartTime;

            long anpStartTime = System.currentTimeMillis();
            for(int i=1; i<10000; i++)
            {
                String name = "Astyanax prepared for 'me': " + astyanax.findByIdPrepared("me");
            }
            long anpEstimatedTime = System.currentTimeMillis() - anpStartTime;


            System.out.println("Astyanax:"+anEstimatedTime);
            System.out.println("Astyanax prepared:"+anpEstimatedTime);
            System.out.println("Datastax:"+dsEstimatedTime);

        } catch(Exception e)
        {
            System.out.println("Exception: " + e.getMessage());
        }

        System.out.println("THE END");
    }
}
