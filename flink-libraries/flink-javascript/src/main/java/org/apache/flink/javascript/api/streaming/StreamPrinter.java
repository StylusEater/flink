package org.apache.flink.javascript.api.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Simple utility class to print all contents of an inputstream to stdout.
 */
public class StreamPrinter extends Thread {
	private final BufferedReader reader;
	private final boolean wrapInException;
	private StringBuilder msg;

	public StreamPrinter(InputStream stream) {
		this(stream, false, null);
	}

	public StreamPrinter(InputStream stream, boolean wrapInException, StringBuilder msg) {
		this.reader = new BufferedReader(new InputStreamReader(stream));
		this.wrapInException = wrapInException;
		this.msg = msg;
	}

	@Override
	public void run() {
		String line;
		try {
			if (wrapInException) {
				while ((line = reader.readLine()) != null) {
					msg.append("\n" + line);
				}
			} else {
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					System.out.flush();
				}
			}
		} catch (IOException ex) {
		}
	}
}

