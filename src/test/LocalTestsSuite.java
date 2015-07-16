package test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ Test_Main_UpdateData.class,
		Test_SparkHelper_CreateLocalSparkContext.class })
public class LocalTestsSuite {

}
