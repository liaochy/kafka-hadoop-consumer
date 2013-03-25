package com.sohu.cyril;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sohu.cyril.io.EtlFile;
import com.sohu.cyril.tools.EtlUtils;
import com.sohu.cyril.tools.PropertiesLoader;
import com.sohu.cyril.tools.Sleeper;

public class TopicConsumer implements Runnable, Stoppable {

	public static Log logger = LogFactory.getLog(TopicConsumer.class);

	private final String topic;

	private final PropertiesLoader loader;

	private final boolean isOffsetExists;

	private final EtlFile file;

	private volatile boolean stopped = false;

	private final Sleeper sleeper;

	public TopicConsumer(String topic, boolean isOffsetExists,
			List<RotateListener> listeners, PropertiesLoader loader)
			throws IOException {
		super();
		this.topic = topic;
		this.loader = loader;
		this.isOffsetExists = isOffsetExists;
		file = new EtlFile(loader, topic, listeners);
		int msgInterval = JobConfiguration.create().getInt(
				"job.server.interval", 3 * 1000);
		sleeper = new Sleeper(msgInterval, this);
	}

	private Properties createConsumerProperties() {
		Properties props = new Properties();
		props.put("zk.connect", EtlUtils.getZkHosts(loader));
		props.put("zk.sessiontimeout.ms",
				String.valueOf(EtlUtils.getZkSessionTimeout(loader)));
		if (EtlJob.init || !isOffsetExists)
			props.put("autooffset.reset", "largest");
		props.put("autocommit.enable", "true");
		props.put("socket.buffersize",
				loader.getProperty("kafka.client.buffer.size"));
		props.put("fetch.size", loader.getProperty("kafka.client.buffer.size"));
		props.put("groupid", loader.getProperty("kafka.groupid"));
		return props;
	}

	public void run() {
		Properties props = createConsumerProperties();
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector
				.createMessageStreams(topicMap);
		List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
		KafkaStream<Message> stream = streams.get(0);
		MessageConsumer consumer = new MessageConsumer(stream, file, topic);
		new Thread(consumer).start();
		for (; !this.stopped;) {
			long now = System.currentTimeMillis();
			sleeper.sleep(now);
		}
		consumer.stop();
		consumerConnector.commitOffsets();
		consumerConnector.shutdown();
		file.shutdown();
		logger.info(Thread.currentThread().getName() + " exiting");
	}

	private static class MessageConsumer implements Runnable {
		private KafkaStream<Message> stream;
		private EtlFile file;
		private String topic;
		private boolean stopped = false;

		public MessageConsumer(KafkaStream<Message> _stream, EtlFile _file,
				String _topic) {
			this.stream = _stream;
			this.file = _file;
			this.topic = _topic;
		}

		public void stop() {
			this.stopped = true;
		}

		public void run() {
			for (MessageAndMetadata<Message> msgAndMetadata : stream) {
				if (!stopped) {
					try {
						file.write(msgAndMetadata.message());
					} catch (IOException e) {
						logger.error("fail to append file,topic is " + topic);
					}
				}
			}
		}

	}

	public void stop(String msg) {
		this.stopped = true;
		logger.info("STOPPED: " + msg);
	}

	public boolean isStopped() {
		return this.stopped;
	}
}
