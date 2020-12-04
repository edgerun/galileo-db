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

