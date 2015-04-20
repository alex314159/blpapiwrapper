blpapiwrapper
=============

Simple Python wrapper around the Python Open Bloomberg API

Requisites:
* blpapi Python library (http://www.bloomberglabs.com/api/libraries/)
* pandas library (http://pandas.pydata.org/)

This wrapper allows simple use of the Bloomberg Python API in a way that emulates the Excel API bdp and bdh functions. It only works if you're connected to Bloomberg, typically on a machine where the Bloomberg terminal application is running.

The Response/Request paradigm and the Subscription paradigm are both implemented. 

For the Response/Request paradigm the bdp output comes as a string, the bdh output comes as pandas DataFrame. Check the main() function for examples.

For the Subscription paradigm the class instance needs to be initialized then one needs to run class.start() to get streaming data. The variable class.output is a pandas DataFrame with the latest data.
