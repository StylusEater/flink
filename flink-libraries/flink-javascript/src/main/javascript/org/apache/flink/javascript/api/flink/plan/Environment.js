/*
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
*/

/* This module is patterned after the Python plan/Environment class. */

/* require([
        'connection/Connection',
        'connection/Collector',
        'plan/DataSet',
        'plan/Constants',
        'plan/OperationInfo',
        'utilities/Switch']); */

load('connection/Connection.js');
load('connection/Collector.js');
load('plan/DataSet.js');
load('plan/Constants.js');
load('plan/OperationInfo.js');
load('utilities/Switch.js');

//import copy
//import sys
//from struct import pack

function get_environment(){
    return Environment();
}

var Environment = (function() {
    var module = {};
    
    // util
    module._counter = 0;
    
    // parameters
    module._dop = -1;
    module._local_mode = Boolean(0);
    module._debug_mode = Boolean(0);
    module._retry = 0

    // sets
    module._sources = [];
    module._sets = [];
    module._sinks = [];

    // specials
    module._broadcast = [];
    module._types = [];
    
    module.register_type = function(type, serializer, deserializer){
        //Registers the given type with this environment, allowing all operators within to
        //(de-)serialize objects of the given type.

        //:param type: class of the objects to be (de-)serialized
        //:param serializer: instance of the serializer
        //:param deserializer: instance of the deserializer
        //self._types.append((pack(">i",126 - len(self._types))[3:], type, serializer, deserializer))
    };
    
    module.read_csv = function(path, types, line_delimiter, field_delimiter){
        var line_delimiter = "\n";
        var field_delimiter = ',';
        
        // https://github.com/evanplaice/jquery-csv
        
        //Create a DataSet that represents the tuples produced by reading the given CSV file.
        //:param path: The path of the CSV file.
        //:param types: Specifies the types for the CSV fields.
        //:return:A CsvReader that can be used to configure the CSV input.
        //child = OperationInfo()
        //child_set = DataSet(self, child)
        //child.identifier = _Identifier.SOURCE_CSV
        //child.delimiter_line = line_delimiter
        //child.delimiter_field = field_delimiter
        //child.path = path
        //child.types = types
        //self._sources.append(child)
        //return child_set
        
    };
    
    module.read_text = function(path){
        //Creates a DataSet that represents the Strings produced by reading the given file line wise.
        //The file will be read with the system's default character set.
        //:param path: The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
        //:return: A DataSet that represents the data read from the given file as text lines.
        //child = OperationInfo()
        //child_set = DataSet(self, child)
        //child.identifier = _Identifier.SOURCE_TEXT
        //child.path = path
        //self._sources.append(child)
        //return child_set
        
    };
    
    module.from_elements = function(elements){
        //Creates a new data set that contains the given elements.
        //The elements must all be of the same type, for example, all of the String or Integer.
        //The sequence of elements must not be empty.
        //:param elements: The elements to make up the data set.
        //:return: A DataSet representing the given list of elements.
        //child = OperationInfo()
        //child_set = DataSet(self, child)
        //child.identifier = _Identifier.SOURCE_VALUE
        //child.values = elements
        //self._sources.append(child)
        //return child_set
    
    };
    
    return module;
});