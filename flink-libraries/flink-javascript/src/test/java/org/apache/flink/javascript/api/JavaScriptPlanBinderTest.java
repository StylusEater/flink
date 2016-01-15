/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.javascript.api;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import static org.apache.flink.javascript.api.JavaScriptPlanBinder.ARGUMENT_JAVASCRIPT_ENGINE;

import org.apache.flink.test.util.JavaProgramTestBase;

public class JavaScriptPlanBinderTest extends JavaProgramTestBase {
	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	private static List<String> findTestFiles() throws Exception {
		List<String> files = new ArrayList();
		FileSystem fs = FileSystem.getLocalFileSystem();
		StringJoiner srcPath = new StringJoiner(File.separator);
		srcPath.add("src").add("test").add("javascript").add("org").add("apache").add("flink").add("javascript").add("api");
		FileStatus[] status = fs.listStatus(new Path( fs.getWorkingDirectory().toString() + File.separator + srcPath.toString()) );
		for (FileStatus f : status) {
			String file = f.getPath().toString();
			if (file.endsWith(".js")) {
				files.add(file);
			}
		}
		return files;
	}

	private static boolean isJavaScriptSupported() {
		try {
			Runtime.getRuntime().exec("jjs");
			return true;
		} catch (IOException ex) {
			return false;
		}
	}

	@Override
	protected void testProgram() throws Exception {
		for (String file : findTestFiles()) {
			JavaScriptPlanBinder.main(new String[]{ARGUMENT_JAVASCRIPT_ENGINE, file});
		}
	}
}

