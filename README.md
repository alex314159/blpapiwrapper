blpapiwrapper
=============

Simple Python wrapper for the Python Open Bloomberg API

Requisites:
* blpapi Python library (https://www.bloomberg.com/professional/support/api-library/)
* pandas library (http://pandas.pydata.org/)

This wrapper allows simple use of the Bloomberg Python API, both terminal based and server based (SAPI):
* the terminal version only works if you're connected to Bloomberg, typically on a machine where the Bloomberg terminal application is running and you are logged in;
* the SAPI version needs a Bloomberg SAPI license as well as the details of any user that is logged into the terminal at the time of the request.

There are three main components:
* a simple implementation that emulates the Excel API bdp and bdh functions, useful for scripting;
* a thread-safe implementation of the Response/Request paradigm; and
* a thread-safe implementation of the Subscription paradigm.

For the Response/Request paradigm the bdp output comes as a string, the bdh output comes as pandas DataFrame. Check the main() function for examples.

The Observer pattern is also implemented for the subscription paradigm.

Tested on Python 2.7 32-bit, Python 3.6.5 64-bit, and Python 3.8 64-bit, with pandas 1.05.

Note: blpapi installation issue on Windows 10 with Python 3.7: please check my answer on https://stackoverflow.com/questions/52897576/install-error-for-blpapi-in-python-for-bloomberg-api/54186235#54186235
