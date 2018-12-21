import java.sql.{DriverManager, ResultSet}
import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD


object mySqlToCassandra extends App {

// 1. Setup Spark Context & sql connection details
  val conf = new SparkConf()
                    .set("spark.cassandra.connection.host", "10.10.173.80") // from Cassandra.yaml --> listen_address: 10.10.173.80
                    .set("spark.cassandra.auth.username", "kalpanav")
                    .set("spark.cassandra.auth.password", "******") 
  val sc = new SparkContext("local[2]", "MigrateMySQLToCassandra", conf)

  val mysqlJdbcString: String = s"jdbc:mysql://wdc-prd-countingmdb-001.openmarket.com:3306/count?user=kalpanav&password=*****"
  Class.forName("com.mysql.jdbc.Driver").newInstance


// 2. Create Cassandra keyspace & tables if not already there
  CassandraConnector(conf).withSessionDo { session =>   
    session.execute("CREATE KEYSPACE IF NOT EXISTS count WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'WDC' : 1, 'VDC' : 1, 'PDC' : 1}")
    session.execute("CREATE TABLE IF NOT EXISTS count.account_by_count (count_account_id TEXT, count_counter counter,PRIMARY key (count_account_id))") 
    session.execute("CREATE TABLE IF NOT EXISTS count.account_by_creation_date(count_account_id TEXT, count_created_date TIMESTAMP, PRIMARY key (count_account_id))") 
  }

 

  val highestId: Long = 100
  val startingId: Long = 0
  val numberOfPartitions = 6;

  val cassandraKeySpace: String = "count";
  val cassandraTableCount: String = "account_by_count";
  val cassandraTableDate: String = "account_by_date";

  // 3. get from MySql & load into Spark
  val sqlCounts = new JdbcRDD(sc, () => { DriverManager.getConnection(mysqlJdbcString)},
    "select id, value from count ", startingId, highestId, numberOfPartitions,
    (r: ResultSet) => {
      (r.getString("id"),
        r.getInt("value")        
      )
    })
 
  val sqlCountsDate = new JdbcRDD(sc, () => { DriverManager.getConnection(mysqlJdbcString)},
    "select id, created_date from count ", startingId, highestId, numberOfPartitions,
    (r: ResultSet) => {
      (r.getString("id"),
       r.getTimestamp("created_date")        
      )
    })

  // 4. Get from Spark & load into Cassandra
  sqlCounts.saveToCassandra(cassandraKeySpace, cassandraTableCount,
      SomeColumns("count_account_id", "count_counter"))

  sqlCountsDate.saveToCassandra(cassandraKeySpace, cassandraTableDate,
      SomeColumns("count_account_id", "count_created_date"))

}
