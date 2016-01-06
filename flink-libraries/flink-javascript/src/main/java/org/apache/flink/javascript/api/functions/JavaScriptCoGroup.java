package org.apache.flink.javascript.api.functions;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.javascript.api.streaming.JavaScriptStreamer;
import org.apache.flink.util.Collector;
import java.io.IOException;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * CoGroupFunction that uses a javascript script.
 *
 * @param <IN1>
 * @param <IN2>
 * @param <OUT>
 */
public class JavaScriptCoGroup<IN1, IN2, OUT> extends RichCoGroupFunction<IN1, IN2, OUT> implements ResultTypeQueryable {
	private final JavaScriptStreamer streamer;
	private transient final TypeInformation<OUT> typeInformation;

	public JavaScriptCoGroup(int id, TypeInformation<OUT> typeInformation) {
		this.typeInformation = typeInformation;
		streamer = new JavaScriptStreamer(this, id);
	}

	/**
	 * Opens this function.
	 *
	 * @param config configuration
	 * @throws IOException
	 */
	@Override
	public void open(Configuration config) throws IOException {
		streamer.open();
		streamer.sendBroadCastVariables(config);
	}

	/**
	 * Calls the external javascript function.
	 *
	 * @param first
	 * @param second
	 * @param out collector
	 * @throws IOException
	 */
	@Override
	public final void coGroup(Iterable<IN1> first, Iterable<IN2> second, Collector<OUT> out) throws Exception {
		streamer.streamBufferWithGroups(first.iterator(), second.iterator(), out);
	}

	/**
	 * Closes this function.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		streamer.close();
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return typeInformation;
	}
}

