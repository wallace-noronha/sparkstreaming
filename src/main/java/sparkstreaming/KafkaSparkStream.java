package sparkstreaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaSparkStream {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream").setMaster("local[*]");
		sparkConf.set("es.input.json","true");		
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.resource" , "/test/_doc");
		sparkConf.set("es.nodes","127.0.0.1");
		sparkConf.set("es.port","9200");
		sparkConf.set("es.nodes.wan.only", "true");
				
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("group.id","1");
		Set<String> topicName = Collections.singleton("test");
		JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicName);
		JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String, String> tuple2) {
			      return tuple2._2();
			     }});
	
		kafkaSparkInputDStream.foreachRDD(rdd -> JavaEsSpark.saveToEs(rdd, "/test/_doc/"));
		
		kafkaSparkInputDStream.print();
		ssc.start();
		
		ssc.awaitTermination();
		 
	}
	
}
