Galileo DB: Galileo Experiment System Database
==============================================

[![PyPI](https://img.shields.io/pypi/v/galileo-db)](https://pypi.org/project/galileo-db/)

This project provides an API to work with the experiment database (edb) of Galileo.
Specifically, it provides the following functionality:

* Core model of the database (Experiment, Telemetry, NodeInfo, ...)
* Various bindings to an actual database (sqlite, mysql)
* Telemetry subscriber to write telemetry into edb.
* Trace recorder

