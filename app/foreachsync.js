"use strict";
var fs = require("fs");
var moment = require("moment");
var eol = require('os').EOL
var path = require("path");
var sleep = require("system-sleep");

var LineByLineReader = require('line-by-line');

const dataDirectory = "data";
const outputDirectory = "output";
const configDirectory = "config";
const inPatientDictionary = "inpatient-data-dictionary.json";
const outPatientDictionary = "outpatient-data-dictionary.json";

var ddPath = "";

//get the record types; inpatient or outpatient
var recordType = process.argv[2];
if (recordType == undefined || recordType.length <= 0) {
    console.error(`Missing Record Type: [${recordType}]; \n\n
      valid types are \"inpatient\" or \"outpatient\"\n
      e.g. >node line-transform.js inpatient`);
  return;
}
else {
  if (recordType != "inpatient" && recordType != "outpatient") {
    console.error(`Unrecognized Record Type: [${recordType}]; \n\n
      valid types are \"inpatient\" or \"outpatient\"\n
      e.g. >node line-transform.js inpatient`);
    return;
  }
  else {
    switch (recordType) {
      case "inpatient":
        ddPath = "./config/inpatient-data-dictionary.json";
        break;
      case "outpatient":
        ddPath = "./config/outpatient-data-dictionary.json";
        break;
    }
  }
}

//load the data dictionary
var dd = require(ddPath);

fs.readdirSync(dataDirectory).forEach(function(file) { 

  var fileTime = "file: " + file;
  console.time(fileTime);

  var dataFile = path.join(dataDirectory, file)
  var outputFile = path.join(outputDirectory, file + ".csv");

  var lr = new LineByLineReader(dataFile);
  var out = fs.createWriteStream(outputFile, "utf8");
  //console.log("df=" + dataFile);
  //console.log("of=" + outputFile);

  lr.on('error', function (err) {
    // 'err' contains error object
          console.log("Error: " + err);
  });

  lr.on('end', function () {
    // All lines are read, file is closed now.
    console.timeEnd(fileTime);
    out.end();
  });

  lr.on('line', function (line) {
          var data = [];

          if (line.length <= 0) return;

          for(var x in dd.fields) {
                  var field = dd.fields[x];
                  //chop up the data line by field
                  data[x] = line.substr((field.startPos - 1), field.length).trim();
                  //var fieldValue = line.substr(field.startPos, field.length);

                  // //check for type
                  // if (!("undefined" == field.type)) {
                  //         switch (field.type) {
                  //                 case "date":
                  //                         fieldValue = moment(fieldValue, field.format.input, true).format(field.format.output);
                  //                         break;
                  //                 default:
                  //
                  //         }
                  // }
                  //data[x] = fieldValue;
          }
          var finalLine = `"${data.join("\",\"")}"`;
          out.write(`${finalLine.replace(/""/g, "")}${eol}`);
          //return `${finalLine.replace(/""/g, "")}${eol}`;
  });
});
