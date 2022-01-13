A quick tool to map sql data to no-SQl Cassandra and handle live data migration of data from sql to no-sql.
it took approx 5 mins to transform and move 5 million records.



# Your directory layout should look like this
$ find .
.
./build.sbt
./src
./src/main
./src/main/scala
./src/main/scala/mySqlToCassandra.scala

# Package a jar containing your application
$ sbt package


	[info] Packaging {..}/{..}/target/scala-2.11/mySqlToCassandra-project_2.11-1.0.jar

# Use spark-submit to run your application

for cassandra test/prod , spark was installed on the cassandra nodes and run from there

$ YOUR_SPARK_HOME/bin/spark-submit \
  --class "mySqlToCassandra" \
  --master local[4] \
  target/scala-2.11/mySqlToCassandra-project_2.11-1.0.jar




