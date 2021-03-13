# snowflake_test

Compile code
```
mvn clean package
```

Run in Spark
```
spark-submit --class TestSnowflake --master spark://<host>:7077 
--packages net.snowflake:snowflake-jdbc:3.12.5,net.snowflake:spark-snowflake_2.12:2.8.4-spark_3.0 
--executor-memory 1G --total-executor-cores 1 target/sample-1.0-SNAPSHOT.jar
```
