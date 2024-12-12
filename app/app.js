'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')
const url = new URL(process.argv[3]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? 'http' ? 80 : 443, // http or https defaults
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

hclient.table("haichenfu_crimes_by_years_hbase").row('2024').get((error, value) => {
	console.info(value)
})

function counterToNumber(c) {
	const buffer = Buffer.from(c, 'latin1');
	if (buffer.length < 8) {
		if (buffer.length === 4) {
			return buffer.readUInt32BE(); // Interpret as a 4-byte integer
		} else if (buffer.length === 2) {
			return buffer.readUInt16BE(); // Interpret as a 2-byte integer
		} else if (buffer.length === 1) {
			return buffer.readUInt8(); // Interpret as a 1-byte integer
		} else {
			throw new Error(`Unsupported buffer length: ${buffer.length}`);
		}
	}
	return Number(buffer.readBigInt64BE());
}
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

app.use(express.static('public'));
app.get('/crimes.html',function (req, res) {
	const year=req.query['year'];
    console.log(year);
	hclient.table("haichenfu_crimes_by_year_hbase").row(year).get(function (err, cells) {
		const crimeInfo = rowToMap(cells);
		console.log(crimeInfo)
		function weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0)
				return " - ";
			return (delays/flights).toFixed(1); /* One decimal place */
		}

		function avg_crime(weather) {
			var crime_amount = crimeInfo["crime:" + weather + "_crime"]
			var weather_amount = crimeInfo["crime:" + weather + "_count"]
			if (weather_amount == 0) {
				return " - "
			}
			return (crime_amount/weather_amount).toFixed(2);
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			year : req.query['year'],
			clear : avg_crime("clear"),
			fog : avg_crime("fog"),
			rain : avg_crime("rain"),
			snow : avg_crime("snow"),
			hail : avg_crime("hail"),
			thunder : avg_crime("thunder"),
			tornado : avg_crime("tornado"),
			visibility_clean: avg_crime("visibility_clean"),
			visibility_moderate: avg_crime("visibility_moderate"),
			visibility_low: avg_crime("visibility_low"),
			visibility_poor: avg_crime("visibility_poor"),
			temp_very_cold: avg_crime("temp_very_cold"),
			temp_cold: avg_crime("temp_cold"),
			temp_chilly: avg_crime("temp_chilly"),
			temp_warm: avg_crime("temp_warm"),
			temp_hot: avg_crime("temp_hot")
		});
		res.send(html);
	});
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[4]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/weather.html', function (req, res) {
	var temp_val = Number(req.query['temperature']);
	var visibility_val = Number(req.query['visibility']);
	var fog_val = (req.query['fog']) ? true : false;
	var rain_val = (req.query['rain']) ? true : false;
	var snow_val = (req.query['snow']) ? true : false;
	var hail_val = (req.query['hail']) ? true : false;
	var thunder_val = (req.query['thunder']) ? true : false;
	var tornado_val = (req.query['tornado']) ? true : false;

	var report = {
		temperature: temp_val,
		visibility: visibility_val,
		clear : !fog_val && !rain_val && !snow_val && !hail_val && !thunder_val && !tornado_val,
		fog : fog_val,
		rain : rain_val,
		snow : snow_val,
		hail : hail_val,
		thunder : thunder_val,
		tornado : tornado_val
	}

	kafkaProducer.send([{ topic: 'haichenfu-weather-report', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-weather.html');
		});
})


app.listen(port);
