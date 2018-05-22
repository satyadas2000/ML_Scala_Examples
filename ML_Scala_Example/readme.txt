export HADOOP_USER_NAME=hadoop
./spark-submit --class com.ML_Scala_Example.kafka.SparkAvroConsumerSaveParquet --jars /home/satya/.m2/repository/org/apache/kafka/kafka-clients/0.10.0.0/kafka-clients-0.10.0.0.jar,/home/satya/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/spark-sql-kafka-0-10_2.11-2.2.0.jar /home/satya/.m2/repository/ML_Scala_Example/ML_Scala_Example/0.0.1-SNAPSHOT/ML_Scala_Example-0.0.1-SNAPSHOT-jar-with-dependencies.jar

./spark-submit --class com.ML_Scala_Example.kafka.SparkLRpredictionAvroMessages --jars /home/satya/.m2/repository/org/apache/kafka/kafka-clients/0.10.0.0/kafka-clients-0.10.0.0.jar,/home/satya/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/spark-sql-kafka-0-10_2.11-2.2.0.jar /home/satya/.m2/repository/ML_Scala_Example/ML_Scala_Example/0.0.1-SNAPSHOT/ML_Scala_Example-0.0.1-SNAPSHOT-jar-with-dependencies.jar

./spark-submit --class com.ML_Scala_Example.LinearRegressionSimple --jars /home/satya/.m2/repository/org/apache/kafka/kafka-clients/0.10.0.0/kafka-clients-0.10.0.0.jar,/home/satya/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/spark-sql-kafka-0-10_2.11-2.2.0.jar /home/satya/.m2/repository/ML_Scala_Example/ML_Scala_Example/0.0.1-SNAPSHOT/ML_Scala_Example-0.0.1-SNAPSHOT-jar-with-dependencies.jar


