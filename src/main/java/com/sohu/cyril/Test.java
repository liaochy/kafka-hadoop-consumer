package com.sohu.cyril;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Iq80DBFactory;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;

public class Test {
	public static void main(String[] args) throws IOException {

		Iq80DBFactory factory = new Iq80DBFactory();
		Options options = new Options();
		options.createIfMissing(true);

		DB db = null;
		try {
			db = factory.open(new File("example"), options);
			for (int i = 0; i < 1000; i++) {
				db.put(bytes("Tampa" + i), bytes("rocks" +i));
			}

			DBIterator iterator = db.iterator();
			try {
				for (iterator.seekToFirst(); iterator.hasNext(); iterator
						.next()) {
					String key = asString(iterator.peekNext().getKey());
					String value = asString(iterator.peekNext().getValue());
					System.out.println(key + " = " + value);
					db.delete(bytes(key));
				}
			} finally {
				// Make sure you close the iterator to avoid resource leaks.
				iterator.close();
			}
		} finally {
			// Make sure you close the db to shutdown the
			// database and avoid resource leaks.
			db.close();
		}
	}

}
