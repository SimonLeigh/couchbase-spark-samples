import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import io.codearte.jfairy.producer.person.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


import java.sql.*;


import java.util.Arrays;
import java.util.List;
import java.util.Random;
import io.codearte.jfairy.*;

import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;


// mysql> create table profiles (givenname VARCHAR(50), surname VARCHAR(50), email VARCHAR(254), entitlementtoken INT(11));
//Query OK, 0 rows affected (0.03 sec)
//
//        mysql> describe profiles;
//        +------------------+--------------+------+-----+---------+-------+
//        | Field            | Type         | Null | Key | Default | Extra |
//        +------------------+--------------+------+-----+---------+-------+
//        | givenname        | varchar(50)  | YES  |     | NULL    |       |
//        | surname          | varchar(50)  | YES  |     | NULL    |       |
//        | email            | varchar(254) | YES  |     | NULL    |       |
//        | entitlementtoken | int(11)      | YES  |     | NULL    |       |
//        +------------------+--------------+------+-----+---------+-------+
//        4 rows in set (0.00 sec)



public class LoadTransformationData {

    private static final int NUM_RECORDS = 10000;
    static Random randomGenerator = new Random();
    static Fairy fairy = Fairy.create();

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/ext_users";
    //  Database credentials
    static final String USER = "profiles";
    static final String PASS = "profiles";

    static Cluster cluster = CouchbaseCluster.create("localhost");
    static Bucket bucket = cluster.openBucket("transformative", "letmein");

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            System.out.println("Connecting to database…");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected to database…");

            System.out.println("Inserting records…");
            stmt = conn.createStatement();

            String sql = "INSERT INTO profiles " +
                    "VALUES ('Matt', 'Ingenthron', 'matt@couchbase.com', 11211)";
            stmt.executeUpdate(sql);
            JsonObject testPerson = JsonObject.create()
                    .put("givenname", "Matt")
                    .put("surname", "Ingenthron")
                    .put("email", "matt@couchbase.com");
            // Store the Document
            bucket.upsert(JsonDocument.create("u:" + "Matt" + "_" + "Ingenthron", testPerson));
            sql = "INSERT INTO profiles " +
                    "VALUES ('Michael', 'Nitschinger', 'michael@nitschinger.at', 11210)";
            testPerson = JsonObject.create()
                    .put("givenname", "Michael")
                    .put("surname", "Nitschinger")
                    .put("email", "michael@nitschinger.at");
            // Store the Document
            bucket.upsert(JsonDocument.create("u:" + "Michael" + "_" + "Nitschinger", testPerson));
            stmt.executeUpdate(sql);

            for(int i=0; i<NUM_RECORDS; i++) {
                int randEntitlement = randEntitlement();
                Person person = fairy.person();

                sql = "INSERT INTO profiles " +
                        "VALUES('" + person.firstName() + "', '" + person.lastName() + "', '" + person.email() + "', " + randEntitlement + ")";
//                System.err.println("Inserting " + sql);
                stmt.executeUpdate(sql);

                // Create a JSON Document
                JsonObject newPerson = JsonObject.create()
                        .put("givenname", person.firstName())
                        .put("surname", person.lastName())
                        .put("email", person.email());
                // Store the Document
                bucket.upsert(JsonDocument.create("u:" + person.firstName() + "_" + person.lastName(), newPerson));


            }

            System.out.println("Inserted records…");

        } catch (SQLException se) {
            se.printStackTrace();
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            try {
                if (stmt != null)
                    conn.close();
            } catch (SQLException se) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        System.exit(0);


    }

    private static int randEntitlement() {
        int randomInt = randomGenerator.nextInt(99999);
        return randomInt;
    }


}
