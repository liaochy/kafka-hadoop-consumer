package com.sohu.cyril;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Observable;

import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.impl.Iq80DBFactory;

import com.sohu.cyril.io.EtlFile;

public class MessageConsumer extends Observable implements Runnable {

	public static Log logger = LogFactory.getLog(MessageConsumer.class);

	private KafkaStream<Message> stream;
	private EtlFile file;
	private String topic;
	private DB leveldb;

	public MessageConsumer(KafkaStream<Message> _stream, EtlFile _file,
			String _topic, DB _leveldb) {
		this.stream = _stream;
		this.file = _file;
		this.topic = _topic;
		this.leveldb = _leveldb;
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
	
	public boolean consumeDBItem(byte[] array) throws IOException{
		return file.write(array);
	}

	@Override
	public void run() {
		for (MessageAndMetadata<Message> msgAndMetadata : stream) {
			try {
				file.write(msgAndMetadata.message());
			} catch (IOException e) {
				logger.error("fail to append file,topic is " + topic
						+ " , exiting");
				leveldb.put(Iq80DBFactory.bytes(topic + ConsumerFactory.splitKey + System.currentTimeMillis()),
						toByteArray(msgAndMetadata.message().payload()));
				stop();
				break;
			}
		}
	}

}