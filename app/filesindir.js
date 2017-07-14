"use strict";
const fs = require("fs");
const moment = require("moment");
const eol = require('os').EOL
const path = require("path");
const crypto = require("crypto");
var LineByLineReader = require('line-by-line');

const dataDirectory = "data";
const outputDirectory = "output";
const configDirectory = "config";
const inPatientDictionary = "inpatient-data-dictionary.json";
const outPatientDictionary = "outpatient-data-dictionary.json";

var ddPath = "";
var keyMap = {};

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
            keyMap = {
              eupidIndex : [2788,22] //{"EUPID","startPos" : 2789,"length" : 22}
              ,admitDateIndex : [785,8] //{"AdmissionDate","startPos" : 786,"length" : 8}
              ,facilityIDIndex : [199,6] //{"FacilityID","startPos" : 200,"length" : 6}
            };
          break;
          case "outpatient":
            ddPath = "./config/outpatient-data-dictionary.json";
            keyMap = {
              eupidIndex : [3288,22] //{"EUPID","startPos" : 3289,"length" : 22}
              ,admitDateIndex : [503,8] //{"AdmissionDate","startPos" : 504,"length" : 8}
              ,facilityIDIndex : [174,6] //{"FacilityID","startPos" : 175,"length" : 6}
            };
          break;
        }
      }
    }

    //load the data dictionary
    var dd = require(ddPath);


    fs.readdir(dataDirectory, function(err, files) {
      if (err) {
        console.error("Could not list the directory", err);
        process.exit(1);
      }

      files.forEach( function (file, index) {
        if (file == ".DS_Store") { return; }

        var fileTime = "file: " + file;
        console.time(fileTime);

        var dataFile = path.join(dataDirectory, file)
        var outputFile = path.join(outputDirectory, file + ".csv");

        var lr = new LineByLineReader(dataFile);
        var out = fs.createWriteStream(outputFile, "utf8");

        var totalrec = 0;
        var goodrec = 0;

        lr.on('error', function (err) {
          // 'err' contains error object
          console.log("Error: " + err);
        });

        lr.on('line', function(line) {
          totalrec++;
          //if line is empty get out
          if (line.length <= 0) return;

          //determine if its a record we can exclude; no eupid
          //{"EUPID"}
          if (line.substr(keyMap.eupidIndex[0], keyMap.eupidIndex[1]).trim().length <= 0) return;

          goodrec++;

          //need to remove double quotes
          //we'll replace with a space to preserve
          //the nature of the fixed-width file
          var cleanLine = line.replace(/"/g, " ");

          //array to hold columns of data
          var data = [];

          //chop up the data line by field
          for(var x in dd.fields) {
            var field = dd.fields[x];
            data[x] = cleanLine.substr((field.startPos - 1), field.length).trim();
          }

          //generate our key
          var rowKey = cleanLine.substr(keyMap.eupidIndex[0], keyMap.eupidIndex[1]).trim() //eupid
                     + cleanLine.substr(keyMap.admitDateIndex[0], keyMap.admitDateIndex[1]).trim() //AdmissionDate
                     + cleanLine.substr(keyMap.facilityIDIndex[0], keyMap.facilityIDIndex[1]).trim(); //FacilityID

          var hashedKey = crypto.createHash("md5").update(rowKey).digest("hex");

          //we'll append the hashed Key as the
          //first column of data to the data row
          var finalLine = `"${hashedKey}","${data.join("\",\"")}"`;

          //output each clean line of data
          //here we'll remove two empty double quotes
          //with simply an empty string
          out.write(`${finalLine.replace(/""/g, "")}${eol}`);
        });

        lr.on('end', function () {
          // All lines are read, file is closed now.
          console.log("totalrecs: " + totalrec + ", goodrecs: " + goodrec);
          console.timeEnd(fileTime);
        });
      });
    });
