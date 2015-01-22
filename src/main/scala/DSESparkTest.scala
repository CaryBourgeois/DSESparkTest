/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Below are the libraries required for this project.
 * In this example all of the dependencies are included with the DSE 4.6 distribution.
 * We need to account for that fact in the build.sbt file in order to make sure we don't introduce
 * library collisions upon deployment to the runtime.
 */

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}

object DSESparkTest {

  /*
   * This is the entry point for the application
   */
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().set("spark.cassandra.connection.host", "192.168.2.101")
      .setJars(Array("target/scala-2.10/DSESparkTest-assembly-1.0.jar"))
      .setMaster("spark://192.168.2.101:7077")
      .setAppName("DSE Spark Test")

    // create a new SparkContext
    val sc = new SparkContext(sparkConf)

    // create a new SparkSQLContext
    val csc = new CassandraSQLContext(sc)

    /*
        In this section we create a native session to Cassandra.
        This is done so that native CQL statements can be executed against the cluster.
     */
    CassandraConnector(sparkConf).withSessionDo { session =>
      /*
       * Make sure that the keyspace we want to use exists and if not create it.
       *
       * Change the topology an replication factor to suit your cluster.
       */
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS dse_spark_test WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}")

      // Minimum Temperature Table
      session.execute(s"DROP TABLE IF EXISTS dse_spark_test.simple")
      session.execute(s"CREATE TABLE IF NOT EXISTS dse_spark_test.simple (pk_id int, value text, PRIMARY KEY(pk_id))")

      //Close the native Cassandra session when done with it. Otherwise, we get some nasty messages in the log.
      session.close()
    }

    //create the data to insert into the table
    val data = sc.parallelize(List((1,"abc"),(2,"def"),(3,"ghi")))
    // Save the data to the cassandra table
    data.saveToCassandra("dse_spark_test", "simple", SomeColumns("pk_id", "value"))

    val bananas = sc.cassandraTable("dse_spark_test", "simple")

    println("Total rows in dse_spark_test.simple: ", bananas.count() )

    // Stop the Spark Context. Otherwise, we get some nasty messages in the log.
    sc.stop()

    System.exit(0)
  }

}
