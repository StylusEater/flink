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

/* This module is patterned after the Python utilities/Switch class. */


var Switch = (function(){
    var module = {};
    
    module.create = function(array_of_items){
        var sw = {};
        
        // Checking for an array is unsupported
        if ( typeof(array_of_items) !== 'object' )
        {
            return null;
        } 
    
        var items = array_of_items;
        var index = 0;
        
        sw.next = function(){
            return index < items.length ?
                items[index++] : Boolean(1);
        };
   
        sw.match = function(item_to_match){
            
            while( value = sw.next() )
            {
                if ( typeof value === 'null' )
                {
                    return Boolean(0);
                }
                
                // Debug
                //console.log(value);
            
                if ( item_to_match.match(new RegExp(value, 'g')) )
                {
                    return Boolean(1);
                }
            }
            
            return Boolean(0);
            
        };
        
        return sw;
    };
        
    return module;
});