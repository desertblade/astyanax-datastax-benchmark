import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class DatastaxImpl implements CassandraDao
{
    private Cluster cluster;
    private Session session;

    public DatastaxImpl()
    {
        PoolingOptions pools = new PoolingOptions();
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, 20);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, 10);
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, 10);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, 5);

        Cluster.Builder builder = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withoutMetrics()
            .withoutJMXReporting()
            .withPoolingOptions(pools);

        session = builder.build().connect();
    }

    @Override
    public String findById(String id) {
        PreparedStatement statement = session.prepare(QueryBuilder.select()
                .from("benchmark", "users")
                .where(QueryBuilder.eq("userid", QueryBuilder.bindMarker())).getQueryString());

        ResultSet results = session.execute(statement.bind(id));
        Row result = results.one();

        return result.getString("firstname") + " " + result.getString("lastname");
    }
}
