import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class AstyanaxImpl implements CassandraDao
{
    public static final ColumnFamily<String, String> CF_USERS = new ColumnFamily<String, String>(
            "users2",
            StringSerializer.get(),
            StringSerializer.get()
    );

    private static Keyspace keyspace;
    private static AstyanaxContext<Keyspace> context;


    public AstyanaxImpl()
    {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forKeyspace("benchmark")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setCqlVersion("3.2.0")
                        .setTargetCassandraVersion("2.1")
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("pool1")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds("127.0.0.1:9160")
                )
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace = context.getClient();
    }

    @Override
    public String findById(String id) throws Exception
    {
        ColumnList<String> result = keyspace.prepareQuery(CF_USERS)
                .getKey(id)
                .execute().getResult();

        return result.getStringValue("firstname", "") + " " + result.getStringValue("lastname", "");
    }

    final String INSERT_STATEMENT = "INSERT INTO employees (empID, deptID, first_name, last_name) VALUES (?, ?, ?, ?);";

    public String findByIdPrepared(String id)      throws Exception
    {
        OperationResult<CqlResult<String, String>> result = keyspace
        .prepareQuery(CF_USERS)
        .withCql("SELECT * FROM users WHERE userid=?")
            .asPreparedStatement()
            .withStringValue(id)
        .execute();

        for (Row<String, String> row : result.getResult().getRows())
        {
            return row.getColumns().getStringValue("firstname", "") + " " + row.getColumns().getStringValue("lastname", "");
        }

        return null;
    }
}
