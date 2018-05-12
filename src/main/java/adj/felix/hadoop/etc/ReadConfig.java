package adj.felix.hadoop.etc;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ReadConfig {
	@Test
	public void readConf() {
		Configuration config = new Configuration();
		config.addResource("etc/config-0.xml");
		
		System.out.println(config.get("color"));
		System.out.println(config.get("weight"));
		System.out.println(config.get("size-weight"));
		System.out.println(config.get("breath", "wide"));
	}
	
	@Test
	public void readFinalConf() {
		Configuration config = new Configuration();
		config.addResource("etc/config-0.xml");
		config.addResource("etc/config-1.xml");
		
		System.out.println(config.get("color"));
		System.out.println(config.get("weight"));
		System.out.println(config.get("size-weight"));
		System.out.println(config.get("breath", "wide"));
	}
}
