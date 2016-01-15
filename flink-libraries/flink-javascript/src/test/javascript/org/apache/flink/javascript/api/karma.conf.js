// Karma configuration
// Generated on Tue Jan 12 2016 16:51:30 GMT-0500 (Eastern Standard Time)

var base = "D:\\projects\\apache\\StylusEater\\flink\\flink-libraries\\flink-javascript\\src\\";
var baseSRC = base.concat("main\\javascript\\org\\apache\\flink\\javascript\\api\\");
var baseTESTS = base.concat("test\\javascript\\org\\apache\\flink\\javascript\\api\\");

module.exports = function(config) {
  config.set({

    // show messages in the browser console
    captureConsole: true,
        
    // base path that will be used to resolve all patterns (eg. files, exclude)
    basePath: base,
    //basePath: './',

    // frameworks to use
    // available frameworks: https://npmjs.org/browse/keyword/karma-adapter
    frameworks: ['jasmine','requirejs'],


    // list of files / patterns to load in the browser
    files: [
        {pattern: baseSRC.concat('**/*.js'), included: true},
        {pattern: baseTESTS.concat('**/*.js'), included: true},
        {pattern: baseSRC.concat('flink/connection/*.js'), included: true},
        {pattern: baseSRC.concat('flink/example/*.js'), included: true},
        {pattern: baseSRC.concat('flink/functions/*.js'), included: true},
        {pattern: baseSRC.concat('flink/plan/*.js'), included: true},
        {pattern: baseSRC.concat('flink/utilities/*.js'), included: true},
        
        {pattern: baseTESTS.concat('flink/connection/*.js'), included: true},
        {pattern: baseTESTS.concat('flink/example/*.js'), included: true},
        {pattern: baseTESTS.concat('flink/functions/*.js'), included: true},
        {pattern: baseTESTS.concat('flink/plan/*.js'), included: true},
        {pattern: baseTESTS.concat('flink/utilities/*.js'), included: true}
    ],


    // list of files to exclude
    exclude: [
      baseTESTS.concat('runtime/*.js'),
      baseTESTS.concat('runtime/**/*.js'),
    ],


    // preprocess matching files before serving them to the browser
    // available preprocessors: https://npmjs.org/browse/keyword/karma-preprocessor
    preprocessors: {
    },


    // test results reporter to use
    // possible values: 'dots', 'progress'
    // available reporters: https://npmjs.org/browse/keyword/karma-reporter
    reporters: ['dots'],


    // web server port
    port: 9876,


    // enable / disable colors in the output (reporters and logs)
    colors: false,


    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_INFO,


    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: true,


    // start these browsers
    // available browser launchers: https://npmjs.org/browse/keyword/karma-launcher
    browsers: ['Chrome'],


    // Continuous Integration mode
    // if true, Karma captures browsers, runs the tests and exits
    singleRun: false,

    // Concurrency level
    // how many browser should be started simultaneous
    concurrency: Infinity
  })
}
