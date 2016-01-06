package org.apache.flink.javascript.api.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.javascript.api.streaming.JavaScriptStreamer;
import org.apache.flink.util.Collector;
import java.io.IOException;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;

/**
 * Multi-purpose class, used for Combine-operations using a javascript script, and as a preprocess step for
 * GroupReduce-operations.
 *
 * @param <IN>
 */
public class JavaScriptCombineIdentity<IN> extends RichGroupReduceFunction<IN, IN> {
	private JavaScriptStreamer streamer;

	public JavaScriptCombineIdentity() {
		streamer = null;
	}

	public JavaScriptCombineIdentity(int id) {
		streamer = new JavaScriptStreamer(this, id);
	}

	@Override
	public void open(Configuration config) throws IOException {
		if (streamer != null) {
			streamer.open();
			streamer.sendBroadCastVariables(config);
		}
	}

	/**
	 * Calls the external javascript function.
	 *
	 * @param values function input
	 * @param out collector
	 * @throws IOException
	 */
	@Override
	public final void reduce(Iterable<IN> values, Collector<IN> out) throws Exception {
		for (IN value : values) {
			out.collect(value);
		}
	}

	/**
	 * Calls the external javascript function.
	 *
	 * @param values function input
	 * @param out collector
	 * @throws IOException
	 */
	@Override
	public final void combine(Iterable<IN> values, Collector<IN> out) throws Exception {
		streamer.streamBufferWithoutGroups(values.iterator(), out);
	}

	@Override
	public void close() throws IOException {
		if (streamer != null) {
			streamer.close();
			streamer = null;
		}
	}
}