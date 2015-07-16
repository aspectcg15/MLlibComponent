package api;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.mllib.recommendation.Rating;
public interface ISqlComponent extends Serializable{

	public void updateSQL(List<Rating> sortedRating);	

	public void connectSQL(int user, double rating, double product, int order);
	
}