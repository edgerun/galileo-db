Galileo DB: Galileo Experiment System Database
==============================================

[![PyPI](https://img.shields.io/pypi/v/galileo-db)](https://pypi.org/project/galileo-db/)

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