/*
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark.sql._

// Airline has subset of the fields that are in the database
case class Airline(name: String, iata: String, icao: String, country: String)

object DatasetExample {

  def main(args: Array[String]): Unit = {
    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("n1qlQueryExample")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.travel-sample", "")

    // Generate The Context
    val sc = new SparkContext(cfg)

    // Spark SQL Setup
    val sql = new SQLContext(sc)
    import sql.implicits._

    val airlines = sql.read.couchbase(schemaFilter = EqualTo("type", "airline")).as[Airline]

    // Print schema
    airlines.printSchema()

    // Print airlines that start with A
    airlines.map(_.name).filter(_.toLowerCase.startsWith("a")).foreach(println(_))
  }

}
