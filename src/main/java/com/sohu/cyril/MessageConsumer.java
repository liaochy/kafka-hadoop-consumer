package com.sohu.cyril;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Observable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import com.sohu.cyril.io.EtlFile;

public class MessageConsumer extends Observable implements Runnable {

	public static Log logger = LogFactory.getLog(MessageConsumer.class);
	public static Log datalogger = LogFactory.getLog("DATA");

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

	public static byte[] toByteArray(ByteBuffer buffer) {
		byte[] ret = new byte[buffer.remaining()];
		buffer.get(ret, 0, ret.length);
		return ret;
	}

	@Override
	public void run() {
		for (MessageAndMetadata<Message> msgAndMetadata : stream) {
			try {
				file.write(msgAndMetadata.message());
			} catch (IOException e) {
				logger.error("fail to append file,topic is " + topic
						+ " , exiting");
				datalogger.info(topic
						+ ":"
						+ new String(toByteArray(msgAndMetadata.message()
								.payload())));
				stop();
				break;
			}
		}
	}

}