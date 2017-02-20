"use strict";

var fileName = process.argv[2];

var LineByLineReader = require('line-by-line'),
        lr = new LineByLineReader("data/" + fileName);

var fs = require("fs");
var moment = require("moment");

const dd = require("/config/dataDictionary");
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
        for(var x in dd.fields) {
                var field = dd.fields[x];
                //chop up the data line by field
                var fieldValue = line.substr(field.startPos, field.length);

                //check for type
                if (!("undefined" == field.type)) {
                        switch (field.type) {
                                case "date":
                                        fieldValue = moment(fieldValue, field.format.input, true).format(field.format.output);
                                        break;
                                default:

                        }
                }
                data[x] = fieldValue;
        }
        ws.write("\"" + data.join("\",\"") + "\"\r\n");
}
