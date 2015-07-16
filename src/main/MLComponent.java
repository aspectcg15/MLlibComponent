package main;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import api.IMLComponent;
import scala.Tuple2;

public class MLComponent implements IMLComponent, Serializable{

	private static final long serialVersionUID = 6757003455071116989L;
	private static final int DEFAULT_RANK = 8;
	private static final int DEFAULT_NUM_ITER = 10;
	private static final double DEFAULT_LAMBDA = 0.01;
	private static final long DEFAULT_RANDOM_SEED = 1237345745L;
	private JavaPairRDD<Tuple2<Integer, Integer>, Double> data;	//user, product key, rating value
	private MatrixFactorizationModel model;
	private int rank;
	private int numIter;
	private double lambda;
	private long randomSeed;
	private ALS als;
	
	public MLComponent(){
		setDefaults();
	}
	
	public MLComponent(JavaPairRDD<Tuple2<Integer, Integer>, Double> data, MatrixFactorizationModel model){
		setDefaults();
		this.data = data;
		this.model = model;
		data.cache();
	}
	
	public MLComponent(JavaPairRDD<Tuple2<Integer, Integer>, Double> data) {
		setDefaults();
		this.data = data;
		data.cache();
	}

	public void updateData(JavaPairRDD<Tuple2<Integer, Integer>, Double> newData){
		if(data == null)
			data = newData;
		else
		//if a value in newData exists for a key use that, otherwise return the old value
		data = data.fullOuterJoin(newData).mapValues((tuVW) -> {return tuVW._2.isPresent()?tuVW._2.get():tuVW._1.get();}).cache();
	}

	public ALS getAls() {
		if(als ==null){
			als = new ALS();
			als.setSeed(randomSeed);
			als.setRank(rank);
			als.setIterations(numIter);
			als.setLambda(lambda);
		}
		
		return als;
	}
	
	public void setAls(ALS als){
		this.als = als;
	}

	@Override
	public JavaPairRDD<Tuple2<Integer, Integer>, Double> getData() {
		return data;
	}

	public void setData(JavaPairRDD<Tuple2<Integer, Integer>, Double> data) {
		this.data = data;
	}

	public MatrixFactorizationModel getModel() {
		return model;
	}

	public void setModel(MatrixFactorizationModel model) {
		this.model = model;
	}
	
	private void setDefaults(){
		rank = DEFAULT_RANK;
		numIter = DEFAULT_NUM_ITER;
		lambda = DEFAULT_LAMBDA;
		randomSeed = DEFAULT_RANDOM_SEED;
	}

	@Override
	public void updateData(JavaRDD<Rating> newData) {
		updateData(newData.mapToPair((rating) -> {return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating());}));
	}

	@Override
	public void refreshModel(){
		if(data == null || data.isEmpty()) return;
		
		ALS temp = getAls();
		
		if(temp == null) return;
		
		//map to RDD of Ratings and perform training
		model = temp.run(data.map((tu2Tu2UPR) -> {return new Rating(tu2Tu2UPR._1._1, tu2Tu2UPR._1._2, tu2Tu2UPR._2);}));
	}

	@Override
	public List<Rating> get5Recommendations(Integer user_id) {
		if(model == null)
			refreshModel();
		if(model == null)
			return null;
		
		JavaRDD<Integer> notRatedByUser = getNotRatedByUser(user_id);
		
		JavaPairRDD<Integer, Double> predictions = notRatedByUser.mapToPair(product -> new Tuple2<Integer, Double>(product, model.predict(user_id, product)));
		
		return predictions.map((tu2ID) -> new Rating(user_id, tu2ID._1, tu2ID._2)).takeOrdered(5, (a, b) -> ((b.rating() - a.rating())<0?-1:(b.rating() - a.rating())==0?0:1));
	}

	private JavaRDD<Integer> getNotRatedByUser(Integer user_id) {
		return data.keys().filter((tu2II) -> tu2II._1 != user_id).map((tu2II) -> tu2II._2).distinct();
	}
}
