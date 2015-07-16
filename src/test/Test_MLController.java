package test;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import main.MLComponent;
import main.MLController;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import api.IMLComponent;
import api.ISqlComponent;
import scala.Tuple2;
import scala.reflect.ClassTag;
import util.SparkHelper;

public class Test_MLController {

	MLController mLCtrl;
	IMLComponent mLComp;
	api.ISqlComponent sqlComp;
	JavaDStreamLike<scala.Tuple2<String,String>,JavaPairDStream<String,String>,JavaPairRDD<String,String>> ks;
	
	
	@Before
	public void setUp() throws Exception {
		mLComp = new MLComponent();
		sqlComp = new MockSqlComp();
		ks = new MockStream();
		mLCtrl = new MLController(ks, mLComp, sqlComp);
	}

	@After
	public void tearDown() throws Exception {
		mLCtrl.close();
	}

	@Test
	public final void testStreamToRatingConversion() {
		try {
			Rating actual = MLController.streamToRatingConversion(new Tuple2<String, String>("topic", "0 1 2.0"));
			Rating expected = new Rating(0,1,2.0);
			assertEquals(expected, actual);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public final void testGetSsc() {
		JavaStreamingContext expected = SparkHelper.createLocalSparkStreamingContext();
		mLCtrl.setSsc(expected);
		JavaStreamingContext actual = mLCtrl.getSsc();
		assertEquals(expected, actual);
		expected.stop();
		expected.close();
	}

	@Test
	public final void testGetKs() {
		JavaDStreamLike<Tuple2<String, String>, JavaPairDStream<String, String>, JavaPairRDD<String, String>> actual = mLCtrl.getKs();
		assertEquals(ks, actual);
	}
	
	@Test
	public final void testSetKs() {
		MockStream expected = new MockStream();
		mLCtrl.setKs(expected);
		JavaDStreamLike<Tuple2<String, String>, JavaPairDStream<String, String>, JavaPairRDD<String, String>> actual = mLCtrl.getKs();
		assertEquals(expected, actual);
	}

	@Test
	public final void testGetmLComp() {
		IMLComponent actual = mLCtrl.getmLComp();
		assertEquals(mLComp, actual);
	}

	@Test
	public final void testSetmLComp() {
		MLComponent expected = new MLComponent();
		mLCtrl.setmLComp(expected);
		IMLComponent actual = mLCtrl.getmLComp();
		assertEquals(expected, actual);
	}

	private static class MockStream implements JavaDStreamLike<scala.Tuple2<String,String>,JavaPairDStream<String,String>,JavaPairRDD<String,String>>{

		private static final long serialVersionUID = 2538513634170709821L;

		@Override
		public DStream<Tuple2<String, String>> checkpoint(Duration arg0) {
			return null;
		}

		@Override
		public ClassTag<Tuple2<String, String>> classTag() {
			return null;
		}

		@Override
		public StreamingContext context() {
			return null;
		}

		@Override
		public JavaDStream<Long> count() {
			return null;
		}

		@Override
		public JavaPairDStream<Tuple2<String, String>, Long> countByValue() {
			return null;
		}

		@Override
		public JavaPairDStream<Tuple2<String, String>, Long> countByValue(
				int arg0) {
			return null;
		}

		@Override
		public JavaPairDStream<Tuple2<String, String>, Long> countByValueAndWindow(
				Duration arg0, Duration arg1) {
			return null;
		}

		@Override
		public JavaPairDStream<Tuple2<String, String>, Long> countByValueAndWindow(
				Duration arg0, Duration arg1, int arg2) {
			return null;
		}

		@Override
		public JavaDStream<Long> countByWindow(Duration arg0, Duration arg1) {
			return null;
		}

		@Override
		public DStream<Tuple2<String, String>> dstream() {
			return null;
		}

		@Override
		public <U> JavaDStream<U> flatMap(
				FlatMapFunction<Tuple2<String, String>, U> arg0) {
			return null;
		}

		@Override
		public <K2, V2> JavaPairDStream<K2, V2> flatMapToPair(
				PairFlatMapFunction<Tuple2<String, String>, K2, V2> arg0) {
			return null;
		}

		@Override
		public void foreach(Function<JavaPairRDD<String, String>, Void> arg0) {
			
		}

		@Override
		public void foreach(
				Function2<JavaPairRDD<String, String>, Time, Void> arg0) {
			
		}

		@Override
		public void foreachRDD(
				Function<JavaPairRDD<String, String>, Void> arg0) {
			
		}

		@Override
		public void foreachRDD(
				Function2<JavaPairRDD<String, String>, Time, Void> arg0) {
			
		}

		@Override
		public JavaDStream<List<Tuple2<String, String>>> glom() {
			return null;
		}

		@Override
		public <R> JavaDStream<R> map(
				Function<Tuple2<String, String>, R> arg0) {
			return null;
		}

		@Override
		public <U> JavaDStream<U> mapPartitions(
				FlatMapFunction<Iterator<Tuple2<String, String>>, U> arg0) {
			return null;
		}

		@Override
		public <K2, V2> JavaPairDStream<K2, V2> mapPartitionsToPair(
				PairFlatMapFunction<Iterator<Tuple2<String, String>>, K2, V2> arg0) {
			return null;
		}

		@Override
		public <K2, V2> JavaPairDStream<K2, V2> mapToPair(
				PairFunction<Tuple2<String, String>, K2, V2> arg0) {
			return null;
		}

		@Override
		public void print() {
			
		}

		@Override
		public void print(int arg0) {
			
		}

		@Override
		public JavaDStream<Tuple2<String, String>> reduce(
				Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> arg0) {
			return null;
		}

		@Override
		public DStream<Tuple2<String, String>> reduceByWindow(
				scala.Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> arg0,
				Duration arg1, Duration arg2) {
			return null;
		}

		@Override
		public JavaDStream<Tuple2<String, String>> reduceByWindow(
				Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> arg0,
				Duration arg1, Duration arg2) {
			return null;
		}

		@Override
		public JavaDStream<Tuple2<String, String>> reduceByWindow(
				Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> arg0,
				Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> arg1,
				Duration arg2, Duration arg3) {
			return null;
		}

		@Override
		public JavaDStream<Long> scalaIntToJavaLong(DStream<Object> arg0) {
			return null;
		}

		@Override
		public List<JavaPairRDD<String, String>> slice(Time arg0, Time arg1) {
			return null;
		}

		@Override
		public <U> JavaDStream<U> transform(
				Function<JavaPairRDD<String, String>, JavaRDD<U>> arg0) {
			return null;
		}

		@Override
		public <U> JavaDStream<U> transform(
				Function2<JavaPairRDD<String, String>, Time, JavaRDD<U>> arg0) {
			return null;
		}

		@Override
		public <K2, V2> JavaPairDStream<K2, V2> transformToPair(
				Function<JavaPairRDD<String, String>, JavaPairRDD<K2, V2>> arg0) {
			return null;
		}

		@Override
		public <K2, V2> JavaPairDStream<K2, V2> transformToPair(
				Function2<JavaPairRDD<String, String>, Time, JavaPairRDD<K2, V2>> arg0) {
			return null;
		}

		@Override
		public <U, W> JavaDStream<W> transformWith(
				JavaDStream<U> arg0,
				Function3<JavaPairRDD<String, String>, JavaRDD<U>, Time, JavaRDD<W>> arg1) {
			return null;
		}

		@Override
		public <K2, V2, W> JavaDStream<W> transformWith(
				JavaPairDStream<K2, V2> arg0,
				Function3<JavaPairRDD<String, String>, JavaPairRDD<K2, V2>, Time, JavaRDD<W>> arg1) {
			return null;
		}

		@Override
		public <U, K2, V2> JavaPairDStream<K2, V2> transformWithToPair(
				JavaDStream<U> arg0,
				Function3<JavaPairRDD<String, String>, JavaRDD<U>, Time, JavaPairRDD<K2, V2>> arg1) {
			return null;
		}

		@Override
		public <K2, V2, K3, V3> JavaPairDStream<K3, V3> transformWithToPair(
				JavaPairDStream<K2, V2> arg0,
				Function3<JavaPairRDD<String, String>, JavaPairRDD<K2, V2>, Time, JavaPairRDD<K3, V3>> arg1) {
			return null;
		}

		@Override
		public JavaPairRDD<String, String> wrapRDD(
				RDD<Tuple2<String, String>> arg0) {
			return null;
		}
		
	}
	
	private static class MockSqlComp implements ISqlComponent{

		@Override
		public void updateSQL(List<Rating> sortedRating) {
		}

		@Override
		public void connectSQL(int user, double rating, double product,
				int order) {
		}
		
	}
}
