package com;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CypherLoader {

	private static Object lock = new Object();
	private static Map<String, String> cache = new HashMap<>();
	private static final Logger LOGGER = LoggerFactory.getLogger(CypherLoader.class);

	public static String get(String filename) throws IOException {

		if (!cache.containsKey(filename)) {

			InputStream inputStream = null;
			try {
				ClassLoader loader = Thread.currentThread().getContextClassLoader();
				inputStream = loader.getResourceAsStream(filename);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				int length = 0;
				while ((length = inputStream.read(buffer)) != -1) {
					baos.write(buffer, 0, length);
				}
				String cypherCode = baos.toString("UTF-8");
				synchronized (lock) {
					cache.put(filename, cypherCode);
				}
			} catch (IOException e) {
				throw e;
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (IOException e) {
						// dont care
						LOGGER.warn("Could not close stream correctly", e);
					}
				}
			}

		}
		return cache.get(filename);

	}
}
