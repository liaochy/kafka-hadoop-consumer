package com.sohu.cyril;

import java.io.IOException;
import java.util.Observable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import com.sohu.cyril.io.EtlFile;

public class MessageConsumer extends Observable implements Runnable {

	public static Log logger = LogFactory.getLog(ConsumerFactory.class);

	private KafkaStream<Message> stream;
	private EtlFile file;
	private String topic;

	public MessageConsumer(KafkaStream<Message> _stream, EtlFile _file,
			String _topic) {
		this.stream = _stream;
		this.file = _file;
		this.topic = _topic;
	}

	public void stop() {
		file.shutdown();
		setChanged();
		notifyObservers(topic);
	}

	@Override
	public void run() {
		for (MessageAndMetadata<Message> msgAndMetadata : stream) {
			try {
				file.write(msgAndMetadata.message());
			} catch (IOException e) {
				logger.error("fail to append file,topic is " + topic
						+ " , existing");
				stop();
				break;
			}
		}
	}

}