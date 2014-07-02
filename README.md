blpapiwrapper
=============

Simple Python wrapper around the Python Open Bloomberg API

Requisites:
* blpapi Python library (http://www.bloomberglabs.com/api/libraries/)
* pandas library (http://pandas.pydata.org/)

This wrapper allows simple use of the Bloomberg Python API in a way that emulates the Excel API bdp and bdh functions. It only works if you're connected to Bloomberg, typically on a machine where the Bloomberg terminal application is running.

The bdp output comes as a string, the bdh output comes as pandas DataFrame. Check the main() function for examples.
