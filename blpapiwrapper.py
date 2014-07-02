"""
Python wrapper to emulate Excel blp and bdh through Bloomberg Open API

Written by Alexandre Almosni
"""


import blpapi
import datetime
import pandas

class BLP():

    def __init__(self):
        self.session = blpapi.Session()
        self.session.start()
        self.session.openService('//BLP/refdata')
        self.refDataSvc = self.session.getService('//BLP/refdata')

    def bdp(self, strSecurity='US900123AL40 Govt', strData='PX_LAST', strOverrideField='', strOverrideValue=''):
        request = self.refDataSvc.createRequest('ReferenceDataRequest')
        request.append('securities', strSecurity)
        request.append('fields',strData)
        if strOverrideField != '':
            o = request.getElement('overrides').appendElement()
            o.setElement('fieldId',strOverrideField)
            o.setElement('value',strOverrideValue)
        requestID = self.session.sendRequest(request)
        continueToLoop = True
        while continueToLoop:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        msgIter = blpapi.event.MessageIterator(event)
        msg = msgIter.next()
        output = msg.getElement('securityData').getValueAsElement(0).getElement('fieldData').getElementAsString(strData)
        if output == '#N/A':
            output = pandas.np.nan
        return output

    def bdh(self, strSecurity='SPX Index', strData='PX_LAST', startdate=datetime.date(2014,1,1), enddate=datetime.date(2014,1,9), periodicity='DAILY'):
        request = self.refDataSvc.createRequest('HistoricalDataRequest')
        request.append('securities', strSecurity)
        if type(strData) == str:
            request.append('fields',strData)
        else:
            for strD in strData:
                request.append('fields',strD)
        request.set('startDate',startdate.strftime('%Y%m%d'))
        request.set('endDate',enddate.strftime('%Y%m%d'))
        request.set('periodicitySelection', periodicity);
        requestID = self.session.sendRequest(request)
        continueToLoop = True
        while (continueToLoop):
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        msgIter = blpapi.event.MessageIterator(event)
        msg = msgIter.next()
        fieldDataArray = msg.getElement('securityData').getElement('fieldData')
        size = fieldDataArray.numValues()
        fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0,size)]
        outDates = [x.getElementAsDatetime('date') for x in fieldDataList]
        if type(strData) == str:
            outData = [x.getElementAsFloat(strData) for x in fieldDataList]
            output = pandas.TimeSeries(data=outData,index=outDates,name=strData)
        else:
            output = pandas.DataFrame(index=outDates,columns=strData)
            for strD in strData:
                outData = [x.getElementAsFloat(strD) for x in fieldDataList]
                output[strD] = outData
        output.replace('#N/A History',pandas.np.nan,inplace=True)
        output.index = output.index.to_datetime()
        return output

    def bdhOHLC(self, strSecurity='SPX Index', startdate=datetime.date(2014,1,1), enddate=datetime.date(2014,1,9), periodicity='DAILY'):
        return self.bdh(strSecurity, ['PX_OPEN','PX_HIGH','PX_LOW','PX_LAST'], startdate, enddate, periodicity)

    def closeSession(self):
        self.session.stop()


def main():
    bloomberg=BLP()
    print bloomberg.bdp()
    print ''
    print bloomberg.bdp('US900123AL40 Govt','YLD_YTM_BID','PX_BID','200')
    print ''
    print bloomberg.bdh()
    print ''
    print bloomberg.bdhOHLC()
    bloomberg.closeSession()

if __name__ == '__main__':
    main()
