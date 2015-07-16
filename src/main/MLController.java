package main;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.mllib.recommendation.Rating;

import api.IMLComponent;
import api.ISqlComponent;
import scala.Tuple2;
import util.RatingHelper;
import util.SparkHelper;

public class MLController implements Serializable, AutoCloseable {

	private static final long serialVersionUID = -695307126047366481L;
	private static final Boolean SEED_DATA = true;
	private JavaStreamingContext ssc;
	private JavaDStreamLike<scala.Tuple2<String,String>,JavaPairDStream<String,String>,JavaPairRDD<String,String>>	ks;
	private IMLComponent mLComp;
	private ISqlComponent sqlComp;
	private String KAFKA_TOPIC = "product_rating";
	private Integer KAFKA_NUM_PARTITIONS = 1;
	
	public MLController(IMLComponent mLComp, ISqlComponent sqlComp) {
		ssc = SparkHelper.createLocalSparkStreamingContext();
		ks = SparkHelper.getKafkaStream(ssc, KAFKA_TOPIC, KAFKA_NUM_PARTITIONS);
		this.mLComp = mLComp;
		this.sqlComp = sqlComp;
	}
	
	public MLController(JavaDStreamLike<scala.Tuple2<String,String>,JavaPairDStream<String,String>,JavaPairRDD<String,String>> ks, IMLComponent mLComp, ISqlComponent sqlComp) {
		this.ks = ks;
		this.mLComp = mLComp;
		this.sqlComp = sqlComp;
	}
	
	public static void main(String[] args) throws Exception{
	    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
	    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

		MLComponent mLComp;
	    if(SEED_DATA)
	    	mLComp = new MLComponent();
	    else
	    	mLComp = new MLComponent();
//		ISqlComponent sqlComp = new SqlComponent();
		MLController mLCtrl = new MLController(mLComp, null);//sqlComp);
		
		if(mLCtrl.getSsc() == null || mLCtrl.getKs() == null){
			mLCtrl.close();
			throw new Exception("Error setting up, key fields are null");
		}
		
		mLCtrl.run();
		mLCtrl.close();
	}

	public void run() {
		ks.foreachRDD(jPRddSS -> {
			if(mLComp == null) return null;

			System.out.println("converting data to rating");
			//convert data to Rating
			JavaRDD<Rating> ratingRdd = jPRddSS.map(tu2SS -> streamToRatingConversion(tu2SS));

			System.out.println("updating data");
			//update data
			mLComp.updateData(ratingRdd);

			System.out.println("updating model");
			//update prediction model
			mLComp.refreshModel();

			if(sqlComp != null){
				System.out.println("getting all users");
				//get all users
				JavaRDD<Integer> users = mLComp.getData().map(tu2tu2IID -> tu2tu2IID._1._1).distinct();

				System.out.println("getting recommendations and updating Sql");
			
				JavaRDD<List<Rating>> recommendationsRdd = users.map(user -> mLComp.get5Recommendations(user));

				recommendationsRdd.foreach(ratingList -> sqlComp.updateSQL(ratingList));
			}else
				System.out.println("sqlComp is null");
			
			return null;
		});
		
		ssc.start();
		ssc.awaitTermination();
	}

	public static Rating streamToRatingConversion(Tuple2<String, String> tu2SS) throws Exception{
		return RatingHelper.convertFromString(tu2SS._2);
	}
	
	public JavaStreamingContext getSsc() {
		return ssc;
	}

	public void setSsc(JavaStreamingContext ssc) {
		this.ssc = ssc;
	}

	public JavaDStreamLike<scala.Tuple2<String,String>,JavaPairDStream<String,String>,JavaPairRDD<String,String>> getKs() {
		return ks;
	}

	public void setKs(JavaDStreamLike<scala.Tuple2<String,String>,JavaPairDStream<String,String>,JavaPairRDD<String,String>> ks) {
		this.ks = ks;
	}

	public IMLComponent getmLComp() {
		return mLComp;
	}

	public void setmLComp(IMLComponent mLComp) {
		this.mLComp = mLComp;
	}

	@Override
	public void close() throws Exception {
		if(ssc != null){
			ssc.stop();
			ssc.close();
		}
	}
}
