package org.apache.flink.javascript.api;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.CoGroupRawOperator;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.CrossOperator.ProjectCross;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.javascript.api.JavaScriptOperationInfo.DatasizeHint;
import static org.apache.flink.javascript.api.JavaScriptOperationInfo.DatasizeHint.HUGE;
import static org.apache.flink.javascript.api.JavaScriptOperationInfo.DatasizeHint.NONE;
import static org.apache.flink.javascript.api.JavaScriptOperationInfo.DatasizeHint.TINY;
import org.apache.flink.javascript.api.JavaScriptOperationInfo.ProjectionEntry;
import org.apache.flink.javascript.api.functions.JavaScriptCoGroup;
import org.apache.flink.javascript.api.functions.JavaScriptCombineIdentity;
import org.apache.flink.javascript.api.functions.JavaScriptMapPartition;
import org.apache.flink.javascript.api.streaming.Receiver;
import org.apache.flink.javascript.api.streaming.StreamPrinter;
import org.apache.flink.runtime.filecache.FileCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows the execution of a Flink plan written in JavaScript.
 */
public class JavaScriptPlanBinder {
	static final Logger LOG = LoggerFactory.getLogger(JavaScriptPlanBinder.class);

	public static final String ARGUMENT_JAVASCRIPT_ENGINE = "jjs";
	
	public static final String FLINK_JAVASCRIPT_DC_ID = "flink";
	public static final String FLINK_JAVASCRIPT_PLAN_NAME = "/plan.js";

	public static final String FLINK_JAVASCRIPT_BINARY_KEY = "nashorn.binary.jjs";
	public static final String PLANBINDER_CONFIG_BCVAR_COUNT = "PLANBINDER_BCVAR_COUNT";
	public static final String PLANBINDER_CONFIG_BCVAR_NAME_PREFIX = "PLANBINDER_BCVAR_";
	public static String FLINK_JAVASCRIPT_BINARY_PATH = GlobalConfiguration.getString(FLINK_JAVASCRIPT_BINARY_KEY, "jjs");

	private static final String FLINK_JAVASCRIPT_FILE_PATH = System.getProperty("java.io.tmpdir") + "/flink_plan";
	private static final String FLINK_JAVASCRIPT_REL_LOCAL_PATH = "/resources/javascript";
	private static final String FLINK_DIR = System.getenv("FLINK_ROOT_DIR");
	private static String FULL_PATH;

	public static StringBuilder arguments = new StringBuilder();

	private Process process;

	private static String FLINK_HDFS_PATH = "hdfs:/tmp";
	public static final String FLINK_TMP_DATA_DIR = System.getProperty("java.io.tmpdir") + "/flink_data";

	public static boolean DEBUG = false;

	private HashMap<Integer, Object> sets = new HashMap();
	public ExecutionEnvironment env;
	private Receiver receiver;

	public static final int MAPPED_FILE_SIZE = 1024 * 1024 * 64;

	/**
	 * Entry point for the execution of a javascript plan.
	 *
	 * @param args planPath[ package1[ packageX[ - parameter1[ parameterX]]]]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/*if (args.length != 1) {
			System.out.println("Usage: ./bin/jsflink.sh <pathToScript>[ <pathToPackage1>[ <pathToPackageX]][ - <parameter1>[ <parameterX>]]");
			return;
		}*/
		//args.forEach( arg -> System.out.println( arg ));
		JavaScriptPlanBinder binder = new JavaScriptPlanBinder();
		binder.runPlan(Arrays.copyOfRange(args, 1, args.length));
	}

	public JavaScriptPlanBinder() throws IOException {
		FLINK_JAVASCRIPT_BINARY_PATH = GlobalConfiguration.getString(FLINK_JAVASCRIPT_BINARY_KEY, "jjs");
		System.out.println(FLINK_JAVASCRIPT_BINARY_PATH);
		FULL_PATH = FLINK_DIR != null
				? FLINK_DIR + FLINK_JAVASCRIPT_REL_LOCAL_PATH //command-line
				: FileSystem.getLocalFileSystem().getWorkingDirectory().toString() //testing
				+ "/src/main/javascript/org/apache/flink/javascript/api";
	}

	private void runPlan(String[] args) throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].compareTo("-") == 0) {
				split = x;
			}
		}

		try {
			prepareFiles(Arrays.copyOfRange(args, 0, split == 0 ? 1 : split));
			startJavaScript(Arrays.copyOfRange(args, split == 0 ? args.length : split + 1, args.length));
			receivePlan();

			if (env instanceof LocalEnvironment) {
				FLINK_HDFS_PATH = "file:" + System.getProperty("java.io.tmpdir") + "/flink";
			}

			distributeFiles(env);
			env.execute();
			close();
		} catch (Exception e) {
			close();
			throw e;
		}
	}

	//=====Setup========================================================================================================
	/**
	 * Copies all files to a common directory (FLINK_JAVASCRIPT_FILE_PATH). This allows us to distribute it as one big
	 * package, and resolves CLASSPATH issues.
	 *
	 * @param filePaths
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private void prepareFiles(String... filePaths) throws IOException, URISyntaxException {
		//Flink javascript package
		String tempFilePath = FLINK_JAVASCRIPT_FILE_PATH;
		clearPath(tempFilePath);
		FileCache.copy(new Path(FULL_PATH), new Path(tempFilePath), false);

		//plan file		
		copyFile(filePaths[0], FLINK_JAVASCRIPT_PLAN_NAME);

		//additional files/folders
		for (int x = 1; x < filePaths.length; x++) {
			copyFile(filePaths[x], null);
		}
	}

	private static void clearPath(String path) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(path));
		if (fs.exists(new Path(path))) {
			fs.delete(new Path(path), true);
		}
	}

	private static void copyFile(String path, String name) throws IOException, URISyntaxException {
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		String identifier = name == null ? path.substring(path.lastIndexOf("/")) : name;
		String tmpFilePath = FLINK_JAVASCRIPT_FILE_PATH + "/" + identifier;
		clearPath(tmpFilePath);
		Path p = new Path(path);
		FileCache.copy(p.makeQualified(FileSystem.get(p.toUri())), new Path(tmpFilePath), true);
	}

	private static void distributeFiles(ExecutionEnvironment env) throws IOException, URISyntaxException {
		clearPath(FLINK_HDFS_PATH);
		FileCache.copy(new Path(FLINK_JAVASCRIPT_FILE_PATH), new Path(FLINK_HDFS_PATH), true);
		env.registerCachedFile(FLINK_HDFS_PATH, FLINK_JAVASCRIPT_DC_ID);
		clearPath(FLINK_JAVASCRIPT_FILE_PATH);
	}

	private void startJavaScript(String[] args) throws IOException {
		for (String arg : args) {
			arguments.append(" ").append(arg);
		}
		receiver = new Receiver(null);
		receiver.open(FLINK_TMP_DATA_DIR + "/output");

		String javascriptBinaryPath = FLINK_JAVASCRIPT_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(javascriptBinaryPath);
		} catch (IOException ex) {
			throw new RuntimeException(javascriptBinaryPath + " does not point to a valid javascript binary.");
		}
		process = Runtime.getRuntime().exec(javascriptBinaryPath + " -B " + FLINK_JAVASCRIPT_FILE_PATH + FLINK_JAVASCRIPT_PLAN_NAME + arguments.toString());

		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}

		try {
			int value = process.exitValue();
			if (value != 0) {
				throw new RuntimeException("Plan file caused an error. Check log-files for details.");
			}
			if (value == 0) {
				throw new RuntimeException("Plan file exited prematurely without an error.");
			}
		} catch (IllegalThreadStateException ise) {//Process still running
		}

		process.getOutputStream().write("plan\n".getBytes());
		process.getOutputStream().write((FLINK_TMP_DATA_DIR + "/output\n").getBytes());
		process.getOutputStream().flush();
	}

	private void close() {
		try { //prevent throwing exception so that previous exceptions aren't hidden.
			if (!DEBUG) {
				FileSystem hdfs = FileSystem.get(new URI(FLINK_HDFS_PATH));
				hdfs.delete(new Path(FLINK_HDFS_PATH), true);
			}

			FileSystem local = FileSystem.getLocalFileSystem();
			local.delete(new Path(FLINK_JAVASCRIPT_FILE_PATH), true);
			local.delete(new Path(FLINK_TMP_DATA_DIR), true);
			receiver.close();
		} catch (NullPointerException npe) {
		} catch (IOException ioe) {
			LOG.error("JavaScriptAPI file cleanup failed. " + ioe.getMessage());
		} catch (URISyntaxException use) { // can't occur
		}
		try {
			process.exitValue();
		} catch (NullPointerException npe) { //exception occurred before process was started
		} catch (IllegalThreadStateException ise) { //process still active
			process.destroy();
		}
	}

	//====Plan==========================================================================================================
	private void receivePlan() throws IOException {
		receiveParameters();
		receiveOperations();
	}

	//====Environment===================================================================================================
	/**
	 * This enum contains the identifiers for all supported environment parameters.
	 */
	private enum Parameters {
		DOP,
		MODE,
		RETRY,
		DEBUG
	}

	private void receiveParameters() throws IOException {
		for (int x = 0; x < 4; x++) {
			Tuple value = (Tuple) receiver.getRecord(true);
			switch (Parameters.valueOf(((String) value.getField(0)).toUpperCase())) {
				case DOP:
					Integer dop = (Integer) value.getField(1);
					env.setParallelism(dop);
					break;
				case MODE:
					FLINK_HDFS_PATH = (Boolean) value.getField(1) ? "file:/tmp/flink" : "hdfs:/tmp/flink";
					break;
				case RETRY:
					int retry = (Integer) value.getField(1);
					env.setNumberOfExecutionRetries(retry);
					break;
				case DEBUG:
					DEBUG = (Boolean) value.getField(1);
					break;
			}
		}
		if (env.getParallelism() < 0) {
			env.setParallelism(1);
		}
	}

	//====Operations====================================================================================================
	/**
	 * This enum contains the identifiers for all supported DataSet operations.
	 */
	protected enum Operation {
		SOURCE_CSV, SOURCE_TEXT, SOURCE_VALUE, SOURCE_SEQ, SINK_CSV, SINK_TEXT, SINK_PRINT,
		PROJECTION, SORT, UNION, FIRST, DISTINCT, GROUPBY, AGGREGATE,
		REBALANCE, PARTITION_HASH,
		BROADCAST,
		COGROUP, CROSS, CROSS_H, CROSS_T, FILTER, FLATMAP, GROUPREDUCE, JOIN, JOIN_H, JOIN_T, MAP, REDUCE, MAPPARTITION
	}

	private void receiveOperations() throws IOException {
		Integer operationCount = (Integer) receiver.getRecord(true);
		for (int x = 0; x < operationCount; x++) {
			String identifier = (String) receiver.getRecord();
			Operation op = null;
			try {
				op = Operation.valueOf(identifier.toUpperCase());
			} catch (IllegalArgumentException iae) {
				throw new IllegalArgumentException("Invalid operation specified: " + identifier);
			}
			if (op != null) {
				switch (op) {
					case SOURCE_CSV:
						createCsvSource(createOperationInfo(op));
						break;
					case SOURCE_TEXT:
						createTextSource(createOperationInfo(op));
						break;
					case SOURCE_VALUE:
						createValueSource(createOperationInfo(op));
						break;
					case SOURCE_SEQ:
						createSequenceSource(createOperationInfo(op));
						break;
					case SINK_CSV:
						createCsvSink(createOperationInfo(op));
						break;
					case SINK_TEXT:
						createTextSink(createOperationInfo(op));
						break;
					case SINK_PRINT:
						createPrintSink(createOperationInfo(op));
						break;
					case BROADCAST:
						createBroadcastVariable(createOperationInfo(op));
						break;
					case AGGREGATE:
						createAggregationOperation(createOperationInfo(op));
						break;
					case DISTINCT:
						createDistinctOperation(createOperationInfo(op));
						break;
					case FIRST:
						createFirstOperation(createOperationInfo(op));
						break;
					case PARTITION_HASH:
						createHashPartitionOperation(createOperationInfo(op));
						break;
					case PROJECTION:
						createProjectOperation(createOperationInfo(op));
						break;
					case REBALANCE:
						createRebalanceOperation(createOperationInfo(op));
						break;
					case GROUPBY:
						createGroupOperation(createOperationInfo(op));
						break;
					case SORT:
						createSortOperation(createOperationInfo(op));
						break;
					case UNION:
						createUnionOperation(createOperationInfo(op));
						break;
					case COGROUP:
						createCoGroupOperation(createOperationInfo(op));
						break;
					case CROSS:
						createCrossOperation(NONE, createOperationInfo(op));
						break;
					case CROSS_H:
						createCrossOperation(HUGE, createOperationInfo(op));
						break;
					case CROSS_T:
						createCrossOperation(TINY, createOperationInfo(op));
						break;
					case FILTER:
						createFilterOperation(createOperationInfo(op));
						break;
					case FLATMAP:
						createFlatMapOperation(createOperationInfo(op));
						break;
					case GROUPREDUCE:
						createGroupReduceOperation(createOperationInfo(op));
						break;
					case JOIN:
						createJoinOperation(NONE, createOperationInfo(op));
						break;
					case JOIN_H:
						createJoinOperation(HUGE, createOperationInfo(op));
						break;
					case JOIN_T:
						createJoinOperation(TINY, createOperationInfo(op));
						break;
					case MAP:
						createMapOperation(createOperationInfo(op));
						break;
					case MAPPARTITION:
						createMapPartitionOperation(createOperationInfo(op));
						break;
					case REDUCE:
						createReduceOperation(createOperationInfo(op));
						break;
				}
			}
		}
	}

	/**
	 * This method creates an OperationInfo object based on the operation-identifier passed.
	 *
	 * @param operationIdentifier
	 * @return
	 * @throws IOException
	 */
	private JavaScriptOperationInfo createOperationInfo(Operation operationIdentifier) throws IOException {
		return new JavaScriptOperationInfo(receiver, operationIdentifier);
	}

	private void createCsvSource(JavaScriptOperationInfo info) throws IOException {
		if (!(info.types instanceof TupleTypeInfo)) {
			throw new RuntimeException("The output type of a csv source has to be a tuple. The derived type is " + info);
		}

		sets.put(info.setID, env.createInput(new TupleCsvInputFormat(new Path(info.path),
				info.lineDelimiter, info.fieldDelimiter, (TupleTypeInfo) info.types), info.types)
				.name("CsvSource"));
	}

	private void createTextSource(JavaScriptOperationInfo info) throws IOException {
		sets.put(info.setID, env.readTextFile(info.path).name("TextSource"));
	}

	private void createValueSource(JavaScriptOperationInfo info) throws IOException {
		sets.put(info.setID, env.fromElements(info.values).name("ValueSource"));
	}

	private void createSequenceSource(JavaScriptOperationInfo info) throws IOException {
		sets.put(info.setID, env.generateSequence(info.from, info.to).name("SequenceSource"));
	}

	private void createCsvSink(JavaScriptOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.writeAsCsv(info.path, info.lineDelimiter, info.fieldDelimiter, info.writeMode).name("CsvSink");
	}

	private void createTextSink(JavaScriptOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.writeAsText(info.path, info.writeMode).name("TextSink");
	}

	private void createPrintSink(JavaScriptOperationInfo info) throws IOException {
		DataSet parent = (DataSet) sets.get(info.parentID);
		parent.output(new PrintingOutputFormat(info.toError));
	}

	private void createBroadcastVariable(JavaScriptOperationInfo info) throws IOException {
		UdfOperator op1 = (UdfOperator) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		op1.withBroadcastSet(op2, info.name);
		Configuration c = ((UdfOperator) op1).getParameters();

		if (c == null) {
			c = new Configuration();
		}

		int count = c.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);
		c.setInteger(PLANBINDER_CONFIG_BCVAR_COUNT, count + 1);
		c.setString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + count, info.name);

		op1.withParameters(c);
	}

	private void createAggregationOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		AggregateOperator ao = op.aggregate(info.aggregates[0].agg, info.aggregates[0].field);

		for (int x = 1; x < info.count; x++) {
			ao = ao.and(info.aggregates[x].agg, info.aggregates[x].field);
		}

		sets.put(info.setID, ao.name("Aggregation"));
	}

	private void createDistinctOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, info.keys.length == 0 ? op.distinct() : op.distinct(info.keys).name("Distinct"));
	}

	private void createFirstOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.first(info.count).name("First"));
	}

	private void createGroupOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.groupBy(info.keys));
	}

	private void createHashPartitionOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.partitionByHash(info.keys));
	}

	private void createProjectOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.project(info.fields).name("Projection"));
	}

	private void createRebalanceOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.rebalance().name("Rebalance"));
	}

	private void createSortOperation(JavaScriptOperationInfo info) throws IOException {
		Grouping op1 = (Grouping) sets.get(info.parentID);
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, ((UnsortedGrouping) op1).sortGroup(info.field, info.order));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, ((SortedGrouping) op1).sortGroup(info.field, info.order));
		}
	}

	private void createUnionOperation(JavaScriptOperationInfo info) throws IOException {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, op1.union(op2).name("Union"));
	}

	private void createCoGroupOperation(JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, new CoGroupRawOperator(
				op1,
				op2,
				new Keys.ExpressionKeys(info.keys1, op1.getType()),
				new Keys.ExpressionKeys(info.keys2, op2.getType()),
				new JavaScriptCoGroup(info.setID, info.types),
				info.types, info.name));
	}

	private void createCrossOperation(DatasizeHint mode, JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		DefaultCross defaultResult;
		switch (mode) {
			case NONE:
				defaultResult = op1.cross(op2);
				break;
			case HUGE:
				defaultResult = op1.crossWithHuge(op2);
				break;
			case TINY:
				defaultResult = op1.crossWithTiny(op2);
				break;
			default:
				throw new IllegalArgumentException("Invalid Cross mode specified: " + mode);
		}
		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, defaultResult.mapPartition(new JavaScriptMapPartition(info.setID, info.types)).name(info.name));
		} else if (info.projections.length == 0) {
			sets.put(info.setID, defaultResult.name("DefaultCross"));
		} else {
			ProjectCross project = null;
			for (ProjectionEntry pe : info.projections) {
				switch (pe.side) {
					case FIRST:
						project = project == null ? defaultResult.projectFirst(pe.keys) : project.projectFirst(pe.keys);
						break;
					case SECOND:
						project = project == null ? defaultResult.projectSecond(pe.keys) : project.projectSecond(pe.keys);
						break;
				}
			}
			sets.put(info.setID, project.name("ProjectCross"));
		}
	}

	private void createFilterOperation(JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new JavaScriptMapPartition(info.setID, info.types)).name(info.name));
	}

	private void createFlatMapOperation(JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new JavaScriptMapPartition(info.setID, info.types)).name(info.name));
	}

	private void createGroupReduceOperation(JavaScriptOperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyGroupReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyGroupReduceOperation((UnsortedGrouping) op1, info));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, applyGroupReduceOperation((SortedGrouping) op1, info));
		}
	}

	private DataSet applyGroupReduceOperation(DataSet op1, JavaScriptOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new JavaScriptCombineIdentity(info.setID * -1))
					.setCombinable(true).name("JavaScriptCombine")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new JavaScriptCombineIdentity())
					.setCombinable(false).name("JavaScriptGroupReducePreStep")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}

	private DataSet applyGroupReduceOperation(UnsortedGrouping op1, JavaScriptOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new JavaScriptCombineIdentity(info.setID * -1))
					.setCombinable(true).name("JavaScriptCombine")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new JavaScriptCombineIdentity())
					.setCombinable(false).name("JavaScriptGroupReducePreStep")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}

	private DataSet applyGroupReduceOperation(SortedGrouping op1, JavaScriptOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new JavaScriptCombineIdentity(info.setID * -1))
					.setCombinable(true).name("JavaScriptCombine")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new JavaScriptCombineIdentity())
					.setCombinable(false).name("JavaScriptGroupReducePreStep")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}

	private void createJoinOperation(DatasizeHint mode, JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && (info.projections == null || info.projections.length == 0)) {
			sets.put(info.setID, createDefaultJoin(op1, op2, info.keys1, info.keys2, mode).name("JavaScriptJoinPreStep")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types)).name(info.name));
		} else {
			DefaultJoin defaultResult = createDefaultJoin(op1, op2, info.keys1, info.keys2, mode);
			if (info.projections.length == 0) {
				sets.put(info.setID, defaultResult.name("DefaultJoin"));
			} else {
				ProjectJoin project = null;
				for (ProjectionEntry pe : info.projections) {
					switch (pe.side) {
						case FIRST:
							project = project == null ? defaultResult.projectFirst(pe.keys) : project.projectFirst(pe.keys);
							break;
						case SECOND:
							project = project == null ? defaultResult.projectSecond(pe.keys) : project.projectSecond(pe.keys);
							break;
					}
				}
				sets.put(info.setID, project.name("ProjectJoin"));
			}
		}
	}

	private DefaultJoin createDefaultJoin(DataSet op1, DataSet op2, String[] firstKeys, String[] secondKeys, DatasizeHint mode) {
		switch (mode) {
			case NONE:
				return op1.join(op2).where(firstKeys).equalTo(secondKeys);
			case HUGE:
				return op1.joinWithHuge(op2).where(firstKeys).equalTo(secondKeys);
			case TINY:
				return op1.joinWithTiny(op2).where(firstKeys).equalTo(secondKeys);
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}
	}

	private void createMapOperation(JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new JavaScriptMapPartition(info.setID, info.types)).name(info.name));
	}

	private void createMapPartitionOperation(JavaScriptOperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.mapPartition(new JavaScriptMapPartition(info.setID, info.types)).name(info.name));
	}

	private void createReduceOperation(JavaScriptOperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyReduceOperation((UnsortedGrouping) op1, info));
		}
	}

	private DataSet applyReduceOperation(DataSet op1, JavaScriptOperationInfo info) {
		return op1.reduceGroup(new JavaScriptCombineIdentity())
				.setCombinable(false).name("JavaScriptReducePreStep")
				.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
				.name(info.name);
	}

	private DataSet applyReduceOperation(UnsortedGrouping op1, JavaScriptOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new JavaScriptCombineIdentity(info.setID * -1))
					.setCombinable(true).name("JavaScriptCombine")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		} else {
			return op1.reduceGroup(new JavaScriptCombineIdentity())
					.setCombinable(false).name("JavaScriptReducePreStep")
					.mapPartition(new JavaScriptMapPartition(info.setID, info.types))
					.name(info.name);
		}
	}
}

