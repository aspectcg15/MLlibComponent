package test;

import static org.junit.Assert.*;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.SparkHelper;

public class Test_SparkHelper_CreateLocalSparkContext {

	JavaSparkContext sc;
	
	@Before
	public void setUp() throws Exception {
		sc = SparkHelper.createLocalSparkContext();
	}

	@After
	public void tearDown() throws Exception {
		if(sc != null){
			sc.stop();
			sc.close();
		}
	}

	@Test
	public final void test() {
		assertTrue(sc instanceof JavaSparkContext);
	}

}
