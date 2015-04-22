"""
Python wrapper to emulate Excel blp and bdh through Bloomberg Open API

Written by Alexandre Almosni
"""


import blpapi
import datetime
import pandas
import threading
from abc import ABCMeta, abstractmethod

class BLP():
    #The Request/Response Paradigm

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

    def __init__(self, strSecurityList=['ESM5 Index','VGM5 Index'], strDataList=['BID','ASK'], floatInterval=0, intCorrIDList=[0,1]):
        #floatInterval is the minimum amount of time before updates - sometimes needs to be set at 0 for things to work properly
        #intCorrID is a user defined ID for the request
        threading.Thread.__init__(self)
        self.session = blpapi.Session()
        self.session.start()
        self.session.openService("//BLP/mktdata")
        if type(strSecurityList)== str:
            strSecurityList=[strSecurityList]
        if type(intCorrIDList)== int:
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
        self.lastUpdateTimeBlmbrg=''#Warning - if you mix live and delayed data you could have non increasing data
        self.lastUpdateTime=datetime.datetime(1900,1,1)
        self.observers = []
    
    def register(self, observer):
        if not observer in self.observers:
            self.observers.append(observer)
 
    def unregister(self, observer):
        if observer in self.observers:
            self.observers.remove(observer)
 
    def unregisterAll(self):
        if self.observers:
            del self.observers[:]
 
    def updateObservers(self, *args, **kwargs):
        for observer in self.observers:
            observer.update(*args, **kwargs)

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
        self.lastUpdateTime=datetime.datetime.now()
        if output.hasElement("EVENT_TIME"):
            self.lastUpdateTimeBlmbrg=output.getElement("EVENT_TIME").toString()
        for i in range(0,len(self.output.columns)):
            field=self.strDataList[i]
            if output.hasElement(field):
                corrID=output.correlationIds()[0].value()
                security=self.dictCorrID[corrID]
                data=output.getElement(field).getValueAsFloat()
                self.output.loc[security,field]=data
                self.updateObservers(time=self.lastUpdateTime, security=security, field=field, corrID=corrID, data=data, bbgTime=self.lastUpdateTimeBlmbrg)

    def handleOtherEvent(self,event):
        #print "Other event: event "+str(event.eventType())
        pass

    def closeSubscription(self):
        self.session.unsubscribe(self.subscriptionList)


class Observer(object):
    __metaclass__ = ABCMeta
 
    @abstractmethod
    def update(self, *args, **kwargs):
        pass

class ObserverExample(Observer):
    def update(self, *args, **kwargs):
        output = kwargs['time'].strftime("%Y-%m-%d %H:%M:%S")+' received '+ kwargs['security'] + ' ' + kwargs['field'] + '=' + str(kwargs['data'])
        output = output + '. CorrID '+str(kwargs['corrID']) + ' bbgTime ' + kwargs['bbgTime']
        print output

def streamPatternExample():
    stream=BLPStream('ESM5 Index','BID',0,1)
    obs=ObserverExample()
    stream.register(obs)
    stream.start()

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
