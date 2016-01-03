blpapiwrapper
=============

Simple Python wrapper for the Python Open Bloomberg API

Requisites:
* blpapi Python library (http://www.bloomberglabs.com/api/libraries/)
* pandas library (http://pandas.pydata.org/)

This wrapper allows simple use of the Bloomberg Python API. It only works if you're connected to Bloomberg, typically on a machine where the Bloomberg terminal application is running.

There are three main components:
* a simple implementation that emulates the Excel API bdp and bdh functions, useful for scripting;
* a thread-safe implementation of the Response/Request paradigm; and
* a thread-safe implementation of the Subscription paradigm.

For the Response/Request paradigm the bdp output comes as a string, the bdh output comes as pandas DataFrame. Check the main() function for examples.

The Observer pattern is also implemented for the subscription paradigm.
