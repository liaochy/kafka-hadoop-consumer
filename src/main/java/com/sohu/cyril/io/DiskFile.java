package com.sohu.cyril.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import kafka.message.Message;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiskFile {
	static final Log LOG = LogFactory.getLog(DiskFile.class);

	public static class Writer implements Closeable {

		public static byte[] toByteArray(ByteBuffer buffer) {
			byte[] ret = new byte[buffer.remaining()];
			buffer.get(ret, 0, ret.length);
			return ret;
		}

		private FileOutputStream outputStream;
		private boolean closeOutputStream = true;
		private AtomicLong curPos = new AtomicLong(0);

		public Writer(File path) throws IOException {
			if (!path.exists() && path.createNewFile())
				this.outputStream = new FileOutputStream(path);
			this.closeOutputStream = true;
		}

		public long append(final Message message) throws IOException {
			byte[] array = toByteArray(message.payload());
			this.outputStream.write(array);
			return curPos.addAndGet(array.length);
		}

		public void close() throws IOException {
			if (this.outputStream == null) {
				return;
			}
			if (this.closeOutputStream) {
				this.outputStream.flush();
				this.outputStream.close();
				this.outputStream = null;
			}
		}
	}
}
