package com.sohu.cyril.io;

import java.io.IOException;
import java.util.List;

import kafka.message.Message;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.sohu.cyril.JobConfiguration;
import com.sohu.cyril.RotateListener;
import com.sohu.cyril.TopicConsumer;
import com.sohu.cyril.tools.EtlUtils;
import com.sohu.cyril.tools.PropertiesLoader;

public class EtlFile {

	private String topic;

	private PropertiesLoader loader;

	private final List<RotateListener> listeners;

	private HdfsFile hdfsFile;

	private final FileSystem fs;

	private final Configuration conf;

	public EtlFile(PropertiesLoader loader, String topic,
			List<RotateListener> listeners) throws IOException {
		super();
		this.topic = topic;
		this.loader = loader;
		this.listeners = listeners;
		conf = JobConfiguration.create();
		this.fs = FileSystem.get(JobConfiguration.create());
		if (this.hdfsFile == null) {
			String fileName = EtlUtils.generateFileName(loader, topic);
			this.hdfsFile = new HdfsFile(fs, new Path(fileName), conf);
		}
	}

	public boolean write(Message message) throws IOException  {
		if (this.hdfsFile == null) {
			String fileName = EtlUtils.generateFileName(loader, topic);
			this.hdfsFile = new HdfsFile(fs, new Path(fileName), conf);
		}
		long pos = this.hdfsFile.append(message);
		String fileName = hdfsFile.getPath().getName();
		for (RotateListener listen : listeners) {
			if (listen.rotateNeeded(pos, fileName)) {
				hdfsFile.close();
				hdfsFile = null;
				break;
			}
		}
		return true;
	}

	public void shutdown() {
		try {
			TopicConsumer.logger.info("closing hdfsFile");
			hdfsFile.close();
		} catch (IOException e) {
			TopicConsumer.logger.error("error closing hdfs file ...", e);
		}
	}
}
