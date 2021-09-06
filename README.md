Galileo DB: Galileo Experiment System Database
==============================================

[![PyPI Version](https://badge.fury.io/py/galileo-db.svg)](https://badge.fury.io/py/galileo-db)
[![Build Status](https://travis-ci.org/edgerun/galileo-db.svg?branch=master)](https://travis-ci.org/edgerun/galileo-db)
[![Coverage Status](https://coveralls.io/repos/github/edgerun/galileo-db/badge.svg?branch=master)](https://coveralls.io/github/edgerun/galileo-db?branch=master)

This project provides an API to work with the experiment database (edb) of Galileo.
Specifically, it provides the following functionality:

* Core model of the database (Experiment, Telemetry, NodeInfo, ...)
* Various bindings to an actual database (sqlite, mysql)
* Telemetry subscriber to write telemetry into edb.
* Trace recorder

Galileo DB Parameters
=============

#### Environment variables

The Galileo DB project allows the following parameters via environment variables.


| Variable | Default | Description |
|----------|---------|---------|
| `galileo_expdb_influxdb_url` | `http://localhost:8086` | The InfluxDB host |
| `galileo_expdb_influxdb_token` | `my-token` | The InfluxDB auth token |
| `galileo_expdb_influxdb_timeout` | `10000` | Time waiting for connection to InfluxDB |
| `galileo_expdb_influxdb_org` | `galileo` | InfluxDB organization |
| `galileo_expdb_influxdb_org_id` | `org-id` | InfluxDB organization |

Run tests
=========

Tests are located in the `tests` package.

To run the InfluxDB tests, you need to be able to connect to a running InfluxDB instance.
Set the connection details via environment variables.
