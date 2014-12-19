import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class DatastaxImpl implements CassandraDao
{
    private Cluster cluster;
    private Session session;

    PreparedStatement findByIdStatement;
    
    public DatastaxImpl()
    {
        Cluster.Builder builder = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withoutMetrics()
            .withoutJMXReporting();

        session = builder.build().connect();
        
        findByIdStatement = session.prepare(QueryBuilder.select()
                .from("benchmark", "users")
                .where(QueryBuilder.eq("userid", QueryBuilder.bindMarker())));
        
    }

    @Override
    public String findById(String id) {

        ResultSet results = session.execute(findByIdStatement.bind(id));
        Row result = results.one();

        return result.getString("firstname") + " " + result.getString("lastname");
    }
}
