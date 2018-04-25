package adj.felix.hadoop.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter {
	private String regex;
	
	public RegexExcludePathFilter(String regex) {
		this.regex = regex;
	}
	@Override
	public boolean accept(Path path) {
		return !path.toString().matches(regex);
	}

}
