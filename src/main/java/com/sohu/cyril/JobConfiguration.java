package com.sohu.cyril;

import org.apache.hadoop.conf.Configuration;

public class JobConfiguration extends Configuration {

	public static JobConfiguration addJobResources(JobConfiguration conf) {
		conf.addResource("job-default.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("job-site.xml");
		return conf;
	}

	public static JobConfiguration create() {
		JobConfiguration conf = new JobConfiguration();
		return addJobResources(conf);
	}

}
