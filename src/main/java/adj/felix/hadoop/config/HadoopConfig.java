package adj.felix.hadoop.config;

public class HadoopConfig {
	private static Config config = Config.getInstance();
	
	/** core-site.xml的fs.defaultFS配置的端口 **/
	public static String HOST = config.get("hdfs.dfs.host");
	public static String PORT = config.get("hdfs.dfs.port");
	
	public static String HDFS_URL = "hdfs://" + HOST  + ":" + PORT;
}
