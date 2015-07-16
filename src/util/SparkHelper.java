package util;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class SparkHelper {

	public static final Duration DEFAULT_BATCH_DURATION = new Duration(30000);
	public static final Integer DEFAULT_LOCAL_THREAD_NUMBER = 2;
	public static final String DEFAULT_APP_NAME = "MLlibComponent";
	public static final String DEFAULT_LOCAL_APP_NAME = "MLlibComponent local";
	private static String DEFAULT_ZK_QUORUM = "10.153.7.113:2184";
	private static String DEFAULT_GROUP_ID = "default_group";
	
	public static JavaSparkContext	createSparkContext(String master){
		return new JavaSparkContext(getDefaultSparkConf(master));
	}
	
	public static JavaSparkContext	createLocalSparkContext(){
		return new JavaSparkContext(getDefaultLocalSparkConf());
	}
	
	public static SparkConf getDefaultSparkConf(String master){
		return new SparkConf().setAppName(DEFAULT_APP_NAME).setMaster(master);
	}
	
	public static SparkConf getDefaultLocalSparkConf(){
		return new SparkConf().setAppName(DEFAULT_LOCAL_APP_NAME).setMaster("local["+DEFAULT_LOCAL_THREAD_NUMBER+"]");
	}

	public static JavaPairReceiverInputDStream<String, String> getKafkaStream(JavaStreamingContext ssc, String topic, Integer numKafkaPartitions) {
		Map<String,Integer>	topicsWithNumPartitions = new HashMap<String,Integer>();
		topicsWithNumPartitions.put(topic, numKafkaPartitions);
		
		return KafkaUtils.createStream(ssc, getZkQuorum(), DEFAULT_GROUP_ID, topicsWithNumPartitions);
	}

	private static String getZkQuorum() {
		return DEFAULT_ZK_QUORUM;
	}

	public static JavaStreamingContext createLocalSparkStreamingContext() {
		return new JavaStreamingContext(getDefaultLocalSparkConf(), DEFAULT_BATCH_DURATION);
	}
}
