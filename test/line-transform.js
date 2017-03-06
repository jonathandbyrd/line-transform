"use strict";
var fs = require("fs");
var moment = require("moment");
var eol = require('os').EOL
const dd = require("./config/inpatient-data-dictionary.json");

var fileName = process.argv[2];
var LineByLineReader = require('line-by-line'),
        lr = new LineByLineReader("data/" + fileName);


var ws = fs.createWriteStream(`output/${fileName}-test.txt`);

console.time("test run");

lr.on('error', function (err) {
	// 'err' contains error object
        console.log("Error: " + err);
});

lr.on('line', processLine);

lr.on('end', function () {
	// All lines are read, file is closed now.
        console.timeEnd("test run");
});

function processLine(line) {
        var data = [];

        if (line.length <= 0) return;

        for(var x in dd.fields) {
                var field = dd.fields[x];
                //chop up the data line by field
                data[x] = line.substr((field.startPos - 1), field.length);
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
        ws.write(`"${data.join("\",\"")}"${eol}`);
}
