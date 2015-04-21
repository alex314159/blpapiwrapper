"""
Python wrapper to emulate Excel bdp and bdh through Bloomberg Open API

Written by Alexandre Almosni
"""


import blpapi
import datetime
import pandas
import threading

class BLP():
    #The Request/Response Paradigm

    def __init__(self):
        #Bloomberg session created only once here - makes consecutive bdp() and bdh() calls faster
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
        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        output = blpapi.event.MessageIterator(event).next().getElement('securityData').getValueAsElement(0).getElement('fieldData').getElementAsString(strData)
        if output == '#N/A':
            output = pandas.np.nan
        return output

    def bdh(self, strSecurity='SPX Index', strData='PX_LAST', startdate=datetime.date(2014,1,1), enddate=datetime.date(2014,1,9), periodicity='DAILY'):
        request = self.refDataSvc.createRequest('HistoricalDataRequest')
        request.append('securities', strSecurity)
        if type(strData) == str:
            strData=[strData]
        for strD in strData:
            request.append('fields',strD)
        request.set('startDate',startdate.strftime('%Y%m%d'))
        request.set('endDate',enddate.strftime('%Y%m%d'))
        request.set('periodicitySelection', periodicity);
        requestID = self.session.sendRequest(request)
        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        fieldDataArray = blpapi.event.MessageIterator(event).next().getElement('securityData').getElement('fieldData')
        fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0,fieldDataArray.numValues())]
        outDates = [x.getElementAsDatetime('date') for x in fieldDataList]
        output = pandas.DataFrame(index=outDates,columns=strData)
        for strD in strData:
            output[strD] = [x.getElementAsFloat(strD) for x in fieldDataList]
        output.replace('#N/A History',pandas.np.nan,inplace=True)
        output.index = output.index.to_datetime()
        return output

    def bdhOHLC(self, strSecurity='SPX Index', startdate=datetime.date(2014,1,1), enddate=datetime.date(2014,1,9), periodicity='DAILY'):
        return self.bdh(strSecurity, ['PX_OPEN','PX_HIGH','PX_LOW','PX_LAST'], startdate, enddate, periodicity)

    def closeSession(self):
        self.session.stop()


class BLPStream(threading.Thread):
    #The Subscription Paradigm
    #The subscribed data will be sitting in self.output and update automatically

    def __init__(self, strSecurityList=['ESM5 Index','VGM5 Index'], strDataList=['BID','ASK'], floatInterval=1, intCorrIDList=[0,1]):
        #floatInterval is the minimum amount of time before updates - sometimes needs to be set at 0 for things to work properly
        #intCorrID is a user defined ID for the request
        threading.Thread.__init__(self)
        self.session = blpapi.Session()
        self.session.start()
        self.session.openService("//BLP/mktdata")
        if type(strSecurityList)== str:
            strSecurityList=[strSecurityList]
        if type(intCorrIDList)== str:
            intCorrIDList=[intCorrIDList]
        if type(strDataList)== str:
            strDataList=[strDataList]
        self.strSecurityList=strSecurityList
        self.strDataList=strDataList
        if len(strSecurityList)!=len(intCorrIDList):
            print 'Number of securities needs to match number of Correlation IDs, overwriting IDs'
            self.intCorrIDList=range(0,len(strSecurityList))
        else:
            self.intCorrIDList=intCorrIDList
        self.subscriptionList=blpapi.subscriptionlist.SubscriptionList()
        for (security, intCorrID) in zip(self.strSecurityList,self.intCorrIDList):
            self.subscriptionList.add(security, self.strDataList, "interval="+str(floatInterval), blpapi.CorrelationId(intCorrID))
        self.output=pandas.DataFrame(index=self.strSecurityList, columns=self.strDataList)
        self.dictCorrID=dict(zip(self.intCorrIDList,self.strSecurityList))
        self.lastUpdate=''#Warning - if you mix live and delayed data you could have non increasing data
        
    def run(self):
        self.session.subscribe(self.subscriptionList)
        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.SUBSCRIPTION_DATA:
                self.handleDataEvent(event)
            else:
                self.handleOtherEvent(event)

    def handleDataEvent(self,event):
        output=blpapi.event.MessageIterator(event).next() 
        if output.hasElement("TIME"):
            self.lastUpdate=output.getElement("TIME").toString()
        for i in range(0,len(self.output.columns)):
            data=self.strDataList[i]
            if output.hasElement(data):
                security=self.dictCorrID[output.correlationIds()[0].value()]
                self.output.loc[security,data]=output.getElement(data).getValueAsFloat()

    def handleOtherEvent(self,event):
        #print "Other event: event "+str(event.eventType())
        pass

    def closeSubscription(self):
        self.session.unsubscribe(self.subscriptionList)


def main():
    ##Examples of the Request/Response Paradigm
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
