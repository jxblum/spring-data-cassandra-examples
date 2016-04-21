package org.springframework.data.cassandra.examples.core.io;

import java.io.Closeable;

@SuppressWarnings("unused")
public abstract class IOUtils {

	public static boolean close(Closeable obj) {
		if (obj != null) {
			try {
				obj.close();
				return true;
			}
			catch (Exception ignore) {
			}
		}

		return false;
	}
}
