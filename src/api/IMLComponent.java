package api;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public interface IMLComponent {
	public void updateData(JavaPairRDD<Tuple2<Integer, Integer>, Double> newData);
	public void updateData(JavaRDD<Rating> newData);
	public void refreshModel();
	public List<Rating> get5Recommendations(Integer user_id);
	public JavaPairRDD<Tuple2<Integer, Integer>, Double> getData();
}
