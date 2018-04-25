package adj.felix.hadoop.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;

public class Config {
	private static ClassLoader classLoader = Config.class.getClassLoader();
	private static Properties configure = new Properties();
	private static final Config INSTANCE = new Config();
	
	private Config() {
		loadConfigure();
	}

	private void loadConfigure () {
		try {
			URL url = classLoader.getResource("config");
			File file = new File(url.getPath());
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(file), "UTF-8");
			configure.load(inStream);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public static Config getInstance () {
		return INSTANCE;
	}
	
	public String get(String key) {
		return configure.getProperty(key, null);
	}
}
