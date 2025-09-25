#!/bin/bash

genjsons(){
	echo generating sample jsonl...

	mkdir -p ./sample.d

	exec 11>&1
	exec 1>./sample.d/tmp.jsonl

	jq -c -n '{tmsg:"2025 Sep 23 01:53:17.012345Z", severity:"INFO", status:200, body:"apt update done"}'
	jq -c -n '{tmsg:"2025 Sep 22 01:53:17.012345Z", severity:"WARN", status:500, body:"apt update failure"}'

	exec 1>&11
	exec 11>&-
}

genavro(){
	echo generating sample avro...

	jname="./sample.d/tmp.jsonl" \
	aname="./sample.d/input.avro" \
		python3 \
			-c 'import pandas; import fastavro; import functools; import os; import operator; functools.reduce(
				lambda state,f: f(state),
				[
					functools.partial(pandas.read_json, lines=True),
					operator.methodcaller("to_dict", "records"),
					lambda records: fastavro.writer(
						open(os.getenv("aname"), "wb"),
						fastavro.parse_schema({
							"name":"Logs",
							"type":"record",
							"fields": [
							  {"name":"tmsg",     "type":"string"},
							  {"name":"severity", "type":"string"},
							  {"name":"status",   "type":"long"},
							  {"name":"body",     "type":"string"},
							],
						}),
						records,
					),
				],
				os.getenv("jname"),
			)'
}

genparquet(){
	echo generating sample parquet file...

	which rs-csv2parquet | fgrep -q rs-csv2parquet || exec sh -c '
		echo rs-csv2parquet missing.
		echo you can install it using cargo install.
		exit 1
	'

	exec 11>&1
	exec 1>./sample.d/tmp.splitn.csv

	echo tmsg,severity,status,body
	echo '2025 Sep 24 01:53:17.012345Z,INFO,200,hello. world'
	echo '2025 Sep 25 01:53:17.012345Z,INFO,200,hello. world'

	exec 1>&11
	exec 11>&-

	rs-csv2parquet \
		--input-csv-filename=./sample.d/tmp.splitn.csv \
		--output-parquet-filename=./sample.d/input.parquet \
		--has-header \
		--same-column-count
}

test -f "./sample.d/tmp.jsonl" || genjsons
test -f "./sample.d/input.avro" || genavro
test -f "./sample.d/input.parquet" || genparquet

export ENV_IN_AVRO_FILENAME=./sample.d/input.avro
export ENV_IN_PARQUET_FILENAME=./sample.d/input.parquet
export ENV_OUT_PARQUET_FILENAME=./sample.d/output.parquet

./concat-avro2parquet

rsql --url 'parquet://./sample.d/output.parquet' -- "
	SELECT
		*
	FROM output
	ORDER BY tmsg
"
