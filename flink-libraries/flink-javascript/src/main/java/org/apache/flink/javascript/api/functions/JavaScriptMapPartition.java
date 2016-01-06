package org.apache.flink.javascript.api.functions;

import java.io.IOException;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.javascript.api.streaming.JavaScriptStreamer;
import org.apache.flink.util.Collector;

/**
 * Multi-purpose class, usable by all operations using a javascript script with one input source and possibly differing
 * in-/output types.
 *
 * @param <IN>
 * @param <OUT>
 */
public class JavaScriptMapPartition<IN, OUT> extends RichMapPartitionFunction<IN, OUT> implements ResultTypeQueryable {
	private final JavaScriptStreamer streamer;
	private transient final TypeInformation<OUT> typeInformation;

	public JavaScriptMapPartition(int id, TypeInformation<OUT> typeInformation) {
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

	@Override
	public void mapPartition(Iterable<IN> values, Collector<OUT> out) throws Exception {
		streamer.streamBufferWithoutGroups(values.iterator(), out);
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

