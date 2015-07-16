package test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import main.MLComponent;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class Test_Main_UpdateData {

	final Double ASSERT_EQUALS_DELTA = 0.001;
	
	JavaSparkContext sc;
	MLComponent main;
	JavaPairRDD<Tuple2<Integer, Integer>, Double> oldDataRdd;
	JavaPairRDD<Tuple2<Integer, Integer>, Double> updateDataRdd;
	
	@Before
	public void setUp() throws Exception {
		sc = util.SparkHelper.createLocalSparkContext();
		
		if(sc == null || main == null)	throw new Exception("test could not setup successfully");
		
		ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> oldDataList = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();
		ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> updateDataList = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>>();

		oldDataList.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(0,0), 0.5));
		oldDataList.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(1,0), 1.5));
		
		updateDataList.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(1,0), 2.5));
		updateDataList.add(new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(1,1), 2.5));

		oldDataRdd = sc.parallelizePairs(oldDataList);
		updateDataRdd = sc.parallelizePairs(updateDataList);
		

		main = new MLComponent(oldDataRdd);
	}

	@After
	public void tearDown() throws Exception {
		if(sc != null){
			sc.stop();
			sc.close();
		}
	}

	@Test
	public final void testUpdateData() {

		List<Tuple2<Tuple2<Integer, Integer>, Double>> dataList = main.getData().collect();
		
		for(Tuple2<Tuple2<Integer, Integer>, Double> entry: dataList){
			if(entry._1().equals(new Tuple2<Integer, Integer>(0,0))){
				assertEquals(0.5, entry._2, ASSERT_EQUALS_DELTA);
			}else if(entry._1().equals(new Tuple2<Integer, Integer>(1,0))){
				assertEquals(1.5, entry._2, ASSERT_EQUALS_DELTA);
			}else{
				fail("unexpected value received");
			}
		}
		
		main.updateData(updateDataRdd);
		
		List<Tuple2<Tuple2<Integer, Integer>, Double>> updatedDataList = main.getData().collect();
		
		for(Tuple2<Tuple2<Integer, Integer>, Double> entry: updatedDataList){
			if(entry._1().equals(new Tuple2<Integer, Integer>(0,0))){
				assertEquals(0.5, entry._2, ASSERT_EQUALS_DELTA);
			}else if(entry._1().equals(new Tuple2<Integer, Integer>(1,0))){
				assertEquals(2.5, entry._2, ASSERT_EQUALS_DELTA);
			}else if(entry._1().equals(new Tuple2<Integer, Integer>(1,1))){
				assertEquals(2.5, entry._2, ASSERT_EQUALS_DELTA);
			}else{
				fail("unexpected value received");
			}
		}
	}
}
