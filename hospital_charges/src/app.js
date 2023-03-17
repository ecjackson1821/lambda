'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
const e = require("express");
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}
function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substring(prefix.length+1)
}

function paidBillsAvg(num_invoices, bills_sum) {
	console.info(num_invoices);
	if(num_invoices == 0)
		return " - ";
	return (bills_sum/num_invoices);
}

function rowToMap(cell) {
	let stat;
	const col = cell['column'];
	if (col == 'charges:max_charge' || col == 'charges:min_charge' || col == 'charges:uninsured_charge' || col == 'charges:num_inputted_bills' || col == 'charges:inputted_bills_sum') {
		stat = counterToNumber(cell['$']);
	}
	else {
		stat = cell['$'];
	}
	return stat;
}

function groupByHospital(procedure_code, cells) {
	let result = [];
	let rushMemorialCharges = {};
	let northwesternCharges = {};
	let numInvoices;
	let sumCharges;
	const notForMustache = ['charges:category', 'charges:code', 'charges:category_code', 'charges:code_description']
	for (const cell of cells) {
		if (notForMustache.includes(cell['column']))
			continue;
		let hospital = removePrefix(cell['key'], procedure_code)
		if (hospital == 'Rush Memorial') {
			rushMemorialCharges[removePrefix(cell['column'], 'charges')] = rowToMap(cell);
		} else if (hospital == 'Northwestern') {
			northwesternCharges[removePrefix(cell['column'], 'charges')] = rowToMap(cell);
		} else {
			console.log("unknown hospital", hospital);
		}
	}
	for (const hospital of[rushMemorialCharges, northwesternCharges]) {
		if (Object.keys(hospital).length !== 0) {
			const avgPaidCharges = paidBillsAvg(hospital['num_inputted_bills'], hospital['inputted_bills_sum'])
			hospital['avg_paid_charges'] = avgPaidCharges;
			delete hospital['num_inputted_bills'];
			delete hospital['inputted_bills_sum'];
		}
		else{
			continue;
		}
	}
	result.push(rushMemorialCharges)
	result.push(northwesternCharges)
	return result;
}

	app.use(express.static('public'));
	app.get('/operation-type.html', function (req, res) {
		// const category=req.query['category'];
		const procedure_code = req.query['procedure_code'];
		hclient.table('chi_hosp').scan(
			{
				filter: {
					type: "PrefixFilter",
					value: procedure_code
				},
				maxVersions: 1
			},
			function (err, cells) {
				let template = filesystem.readFileSync("result.mustache").toString();
				let input = {hospital_charges: groupByHospital(procedure_code, cells)}
				let html = mustache.render(template, {hospital_charges: groupByHospital(procedure_code, cells)});
				res.send(html)
			})

	});

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/input-charges.html',function (req, res){
	var code_hospital = (req.query['procedure_code']+"_"+req.query['hospital']);
	var category_input = req.query['category'];
	var procedure_code_input = req.query['procedure_code'];
	var hospital_input = req.query['hospital'];
	var billed_amount_input = req.query['billed_amount'];
	var report = {
		code_hospital: (procedure_code_input+"_"+hospital_input),
		category : category_input,
		procedure_code : procedure_code_input,
		billed_amount : billed_amount_input
	};

	kafkaProducer.send([{ topic: 'ecjackson_hospital_input', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log("report", report);
			console.log("data", data);
	res.redirect('submit-hospital-bill.html');
		});
});

app.listen(port);