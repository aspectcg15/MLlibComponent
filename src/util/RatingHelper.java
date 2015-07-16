package util;

import org.apache.spark.mllib.recommendation.Rating;

public class RatingHelper {
	
	public static Rating convertFromString(String str) throws Exception{
		return convertFromString(str, " ");
	}
	
	public static Rating convertFromString(String str, String delimiter) throws Exception{
		String[] tokens = str.split(delimiter);
		
		if(tokens.length != 3) throw new Exception("unexpected number of tokens received "+tokens.length);
		
		return new Rating(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]), Double.parseDouble(tokens[2]));
	}
}
