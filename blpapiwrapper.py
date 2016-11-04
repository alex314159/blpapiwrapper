"""
Python wrapper to download data through the Bloomberg Open API
Written by Alexandre Almosni
(C) 2014-2016 Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0
"""


from abc import ABCMeta, abstractmethod
import blpapi
import datetime
import pandas
import threading

#This makes successive requests faster
DATE             = blpapi.Name("date")
ERROR_INFO       = blpapi.Name("errorInfo")
EVENT_TIME       = blpapi.Name("EVENT_TIME")
FIELD_DATA       = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID         = blpapi.Name("fieldId")
SECURITY         = blpapi.Name("security")
SECURITY_DATA    = blpapi.Name("securityData")

################################################
class BLP():
    """Naive implementation of the Request/Response Paradigm closely matching the Excel API.
    Sharing one session for subsequent requests is faster, however it is not thread-safe, as some events can come faster than others.
    bdp returns a string, bdh returns a pandas DataFrame.
    This is mostly useful for scripting, but care should be taken when used in a real world application.
    """

    def __init__(self):
        self.session = blpapi.Session()
        self.session.start()
        self.session.openService('//BLP/refdata')
        self.refDataSvc = self.session.getService('//BLP/refdata')

    def bdp(self, strSecurity='US900123AL40 Govt', strData='PX_LAST', strOverrideField='', strOverrideValue=''):
        request = self.refDataSvc.createRequest('ReferenceDataRequest')
        request.append('securities', strSecurity)
        request.append('fields', strData)

        if strOverrideField != '':
            o = request.getElement('overrides').appendElement()
            o.setElement('fieldId', strOverrideField)
            o.setElement('value', strOverrideValue)

        requestID = self.session.sendRequest(request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        try:
            output = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).getValueAsElement(0).getElement(FIELD_DATA).getElementAsString(strData)
            if output == '#N/A':
                output = pandas.np.nan
        except:
            print 'error with '+strSecurity+' '+strData
            output = pandas.np.nan
        return output

    def bdh(self, strSecurity='SPX Index', strData='PX_LAST', startdate=datetime.date(2014, 1, 1), enddate=datetime.date(2014, 1, 9), adjustmentSplit=False, periodicity='DAILY'):
        request = self.refDataSvc.createRequest('HistoricalDataRequest')
        request.append('securities', strSecurity)
        if type(strData) == str:
            strData = [strData]

        for strD in strData:
            request.append('fields', strD)

        request.set('startDate', startdate.strftime('%Y%m%d'))
        request.set('endDate', enddate.strftime('%Y%m%d'))
        request.set('adjustmentSplit', 'TRUE' if adjustmentSplit else 'FALSE')
        request.set('periodicitySelection', periodicity)
        requestID = self.session.sendRequest(request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break

        fieldDataArray = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).getElement(FIELD_DATA)
        fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0, fieldDataArray.numValues())]
        outDates = [x.getElementAsDatetime(DATE) for x in fieldDataList]
        output = pandas.DataFrame(index=outDates, columns=strData)

        for strD in strData:
            listD = []
            for x in fieldDataList:
                try:
                    listD.append(x.getElementAsFloat(strD))
                except blpapi.exception.NotFoundException:
                    # print('nan found for {}'.format(x.getElement('date')))
                    listD.append(pandas.np.nan)
            output[strD] = listD

        output.replace('#N/A History', pandas.np.nan, inplace=True)
        output.index = output.index.to_datetime()
        return output

    def bdhOHLC(self, strSecurity='SPX Index', startdate=datetime.date(2014, 1, 1), enddate=datetime.date(2014, 1, 9), periodicity='DAILY'):
        return self.bdh(strSecurity, ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST'], startdate, enddate, periodicity)

    def closeSession(self):
        self.session.stop()
################################################


class BLPTS():
    """Thread-safe implementation of the Request/Response Paradigm.
    The functions don't return anything but notify observers of results.
    Including startDate as a keyword argument will define a HistoricalDataRequest, otherwise it will be a ReferenceDataRequest.
    HistoricalDataRequest sends observers a pandas DataFrame, whereas ReferenceDataRequest sends a pandas Series.
    Override seems to only work when there's one security, one field, and one override.
    Examples:
    BLPTS(['ESA Index', 'VGA Index'], ['BID', 'ASK'])
    BLPTS('US900123AL40 Govt','YLD_YTM_BID',strOverrideField='PX_BID',strOverrideValue='200')
    BLPTS(['SPX Index','SX5E Index','EUR Curncy'],['PX_LAST','VOLUME'],startDate=datetime.datetime(2014,1,1),endDate=datetime.datetime(2015,5,14),periodicity='DAILY')
    """

    def __init__(self, securities=[], fields=[], **kwargs):
        """
        Keyword arguments:
        securities : list of ISINS 
        fields : list of fields 
        kwargs : startDate and endDate (datetime.datetime object, note: hours, minutes, seconds, and microseconds must be replaced by 0)
        """
        self.session    = blpapi.Session()
        self.session.start()
        self.session.openService('//BLP/refdata')
        self.refDataSvc = self.session.getService('//BLP/refdata')
        self.observers  = []
        self.kwargs     = kwargs

        if len(securities) > 0 and len(fields) > 0:
            # also works if securities and fields are a string
            self.fillRequest(securities, fields, **kwargs)

    def fillRequest(self, securities, fields, **kwargs):
        """
        keyword arguments:
        securities : list of ISINS
        fields : list of fields 
        kwargs : startDate and endDate (datetime.datetime object, note: hours, minutes, seconds, and microseconds must be replaced by 0)
        """
        self.kwargs = kwargs

        if type(securities) == str:
            securities = [securities]

        if type(fields) == str:
            fields = [fields]

        if 'startDate' in kwargs:
            self.request   = self.refDataSvc.createRequest('HistoricalDataRequest')
            self.startDate = kwargs['startDate']
            self.endDate   = kwargs['endDate']

            if 'periodicity' in kwargs:
                self.periodicity = kwargs['periodicity']
            else:
                self.periodicity = 'DAILY'

            self.request.set('startDate', self.startDate.strftime('%Y%m%d'))
            self.request.set('endDate', self.endDate.strftime('%Y%m%d'))
            self.request.set('periodicitySelection', self.periodicity)

        else:
            self.request = self.refDataSvc.createRequest('ReferenceDataRequest')
            self.output  = pandas.DataFrame(index=securities, columns=fields)

            if 'strOverrideField' in kwargs:
                o = self.request.getElement('overrides').appendElement()
                o.setElement('fieldId', kwargs['strOverrideField'])
                o.setElement('value', kwargs['strOverrideValue'])

        self.securities = securities
        self.fields     = fields

        for s in securities:
            self.request.append('securities', s)

        for f in fields:
            self.request.append('fields', f)

    def get(self, newSecurities=[], newFields=[], **kwargs):
        """
        securities : list of ISINS 
        fields : list of fields 
        kwargs : startDate and endDate (datetime.datetime object, note: hours, minutes, seconds, and microseconds must be replaced by 0)
        """

        if len(newSecurities) > 0 or len(newFields) > 0:
            self.fillRequest(newSecurities, newFields, **kwargs)

        self.requestID = self.session.sendRequest(self.request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() in [blpapi.event.Event.RESPONSE, blpapi.event.Event.PARTIAL_RESPONSE]:
                responseSize = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).numValues()

                for i in range(0, responseSize):

                    if 'startDate' in self.kwargs:
                        # HistoricalDataRequest
                        output         = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA)
                        security       = output.getElement(SECURITY).getValueAsString()
                        fieldDataArray = output.getElement(FIELD_DATA)
                        fieldDataList  = [fieldDataArray.getValueAsElement(i) for i in range(0, fieldDataArray.numValues())]
                        dates          = map(lambda x: x.getElement(DATE).getValueAsString(), fieldDataList)
                        outDF          = pandas.DataFrame(index=dates, columns=self.fields)
                        outDF.index    = outDF.index.to_datetime()

                        for field in self.fields:
                            data = []
                            for row in fieldDataList:
                                if row.hasElement(field):
                                    data.append(row.getElement(field).getValueAsFloat())
                                else:
                                    data.append(pandas.np.nan)

                            outDF[field] = data
                            self.updateObservers(security=security, field=field, data=outDF) # update one security one field

                        self.updateObservers(security=security, field='ALL', data=outDF) # update one security all fields

                    else:
                        # ReferenceDataRequest
                        output   = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).getValueAsElement(i)
                        n_elmts  = output.getElement(FIELD_DATA).numElements()
                        security = output.getElement(SECURITY).getValueAsString()
                        for j in range(0, n_elmts):
                            data     = output.getElement(FIELD_DATA).getElement(j)
                            field    = str(data.name())
                            outData  = _dict_from_element(data)
                            self.updateObservers(security=security, field=field, data=outData) # update one security one field
                            self.output.loc[security, field] = outData
                            
                        if n_elmts>0:
                            self.updateObservers(security=security, field='ALL', data=self.output.loc[security]) # update one security all fields
                        else:
                            print 'Empty response received for ' + security

            if event.eventType() == blpapi.event.Event.RESPONSE:
                break

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

    def closeSession(self):
        self.session.stop()
################################################


class BLPStream(threading.Thread):
    """The Subscription Paradigm
    The subscribed data will be sitting in self.output and update automatically. Observers will be notified.
    floatInterval is the minimum amount of time before updates - sometimes needs to be set at 0 for things to work properly
    intCorrID is a user defined ID for the request
    It is sometimes safer to ask for each data (for instance BID and ASK) in a separate stream.
    Note that for corporate bonds, a change in the ASK price will still trigger a BID event.
    """

    def __init__(self, strSecurityList=['ESM5 Index', 'VGM5 Index'], strDataList=['BID', 'ASK'], floatInterval=0, intCorrIDList=[0, 1]):
        threading.Thread.__init__(self)
        self.session = blpapi.Session()
        self.session.start()
        self.session.openService("//BLP/mktdata")

        if type(strSecurityList) == str:
            strSecurityList = [strSecurityList]

        if type(intCorrIDList) == int:
            intCorrIDList = [intCorrIDList]

        if type(strDataList) == str:
            strDataList = [strDataList]

        self.strSecurityList = strSecurityList
        self.strDataList     = strDataList

        if len(strSecurityList) != len(intCorrIDList):
            print 'Number of securities needs to match number of Correlation IDs, overwriting IDs'
            self.intCorrIDList = range(0, len(strSecurityList))
        else:
            self.intCorrIDList = intCorrIDList

        self.subscriptionList = blpapi.subscriptionlist.SubscriptionList()
        for (security, intCorrID) in zip(self.strSecurityList, self.intCorrIDList):
            self.subscriptionList.add(security, self.strDataList, "interval="+str(floatInterval), blpapi.CorrelationId(intCorrID))

        self.output               = pandas.DataFrame(index=self.strSecurityList, columns=self.strDataList)
        self.dictCorrID           = dict(zip(self.intCorrIDList, self.strSecurityList))
        self.lastUpdateTimeBlmbrg = ''  # Warning - if you mix live and delayed data you could have non increasing data
        self.lastUpdateTime       = datetime.datetime(1900, 1, 1)
        self.observers            = []

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

    def run(self, verbose=False):
        self.session.subscribe(self.subscriptionList)
        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.SUBSCRIPTION_DATA:
                self.handleDataEvent(event)
            else:
                if verbose:
                    self.handleOtherEvent(event)

    def handleDataEvent(self, event):
        output              = blpapi.event.MessageIterator(event).next()
        self.lastUpdateTime = datetime.datetime.now()
        corrID              = output.correlationIds()[0].value()
        security            = self.dictCorrID[corrID]
        isParsed            = False
        #print output.toString()

        if output.hasElement(EVENT_TIME):
            self.lastUpdateTimeBlmbrg = output.getElement(EVENT_TIME).toString()

        for field in self.strDataList:
            if output.hasElement(field):
                isParsed = True
                try:
                    data = output.getElement(field).getValueAsFloat()
                except:
                    data = pandas.np.nan
                    print 'error: ',security,field#,output.getElement(field).getValueAsString() # this can still error if field is there but is empty
                self.output.loc[security, field] = data
                self.updateObservers(time=self.lastUpdateTime, security=security, field=field, corrID=corrID, data=data, bbgTime=self.lastUpdateTimeBlmbrg)
         
        # It can happen that you get an event without the data behind the event!
        self.updateObservers(time=self.lastUpdateTime, security=security, field='ALL', corrID=corrID, data=0, bbgTime=self.lastUpdateTimeBlmbrg)
        # if not isParsed:
        #     print output.toString()

    def handleOtherEvent(self, event):
        output = blpapi.event.MessageIterator(event).next()
        msg = output.toString()
        if event.eventType() == blpapi.event.Event.AUTHORIZATION_STATUS:
            print "Authorization event: " + msg
        elif event.eventType() == blpapi.event.Event.SUBSCRIPTION_STATUS:
            print "Subscription status event: " + msg
        else:
            print "Other event: event "+str(event.eventType())

    def closeSubscription(self):
        self.session.unsubscribe(self.subscriptionList)
################################################
#Convenience functions below####################
################################################


def _dict_from_element(element):
    '''
    Used for e.g. dividends
    '''
    try:
        return element.getValueAsString()
    except:
        if element.numValues() > 1:
            results = []
            for i in range(0, element.numValues()):
                subelement    = element.getValue(i)
                name          = str(subelement.name())
                results.append(_dict_from_element(subelement))
        else:
            results = {}
            for j in range(0, element.numElements()):
                subelement    = element.getElement(j)
                name          = str(subelement.name())
                results[name] = _dict_from_element(subelement)
        return results


class Observer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def update(self, *args, **kwargs):
        pass


class HistoryWatcher(Observer):
    """Object to stream and record history data from Bloomberg.
    """
    def __init__(self):
        self.outputDC={}
    def update(self, *args, **kwargs):
        if kwargs['field']!='ALL':
            self.outputDC[(kwargs['security'],kwargs['field'])]=kwargs['data'][[kwargs['field']]]#double brackets keep it a dataframe, not a series


def simpleReferenceDataRequest(id_to_ticker_dic, fields):
    '''
    Common use case for reference data request
    id_to_ticker_dic: dictionnary with user id mapped to Bloomberg security ticker e.g. {'Apple':'AAPL US Equity'}
    Returns a dataframe indexed by the user id, with columns equal to fields
    '''
    ticker_to_id_dic =  {v: k for k, v in id_to_ticker_dic.items()}
    blpts = BLPTS(id_to_ticker_dic.values(), fields)
    blpts.get()
    blpts.closeSession()
    blpts.output['id'] = blpts.output.index
    blpts.output['id'].replace(ticker_to_id_dic,inplace=True)
    blpts.output.set_index('id', inplace=True)
    return blpts.output.copy()


def simpleHistoryRequest(securities=[], fields=[], startDate=datetime.datetime(2015,1,1), endDate=datetime.datetime(2016,1,1), periodicity='DAILY'):
    '''
    Convenience function to retrieve historical data for a list of securities and fields
    As returned data can have different length, missing data will be replaced with pandas.np.nan (note it's already taken care of in one security several fields)
    If multiple securities and fields, a MultiIndex dataframe will be returned.
    '''
    blpts=BLPTS(securities, fields, startDate=startDate, endDate=endDate, periodicity=periodicity)
    historyWatcher=HistoryWatcher()
    blpts.register(historyWatcher)
    blpts.get()
    blpts.closeSession()
    for key,df in historyWatcher.outputDC.iteritems():
        df.columns=[key]
    output=pandas.concat(historyWatcher.outputDC.values(),axis=1)
    output.columns=pandas.MultiIndex.from_tuples(output.columns)
    output.columns.names=['Security','Field']
    return output


################################################
#Examples below#################################
################################################

def excelEmulationExample():
    ##Examples of the Request/Response Paradigm
    bloomberg = BLP()
    print bloomberg.bdp()
    print ''
    print bloomberg.bdp('US900123AL40 Govt', 'YLD_YTM_BID', 'PX_BID', '200')
    print ''
    print bloomberg.bdh()
    print ''
    print bloomberg.bdhOHLC()
    bloomberg.closeSession()


class ObserverStreamExample(Observer):
    def update(self, *args, **kwargs):
        output = kwargs['time'].strftime("%Y-%m-%d %H:%M:%S") + ' received ' + kwargs['security'] + ' ' + kwargs['field'] + '=' + str(kwargs['data'])
        output = output + '. CorrID '+str(kwargs['corrID']) + ' bbgTime ' + kwargs['bbgTime']
        print output


def streamPatternExample():
    stream = BLPStream('ESM5 Index', ['BID', 'ASK'], 0, 1)
    #stream=BLPStream('XS1151974877 CORP',['BID','ASK'],0,1) #Note that for a bond only BID gets updated even if ASK moves.
    obs = ObserverStreamExample()
    stream.register(obs)
    stream.start()


class ObserverRequestExample(Observer):
    def update(self, *args, **kwargs):
        if kwargs['field'] == 'ALL':
            print kwargs['security']
            print kwargs['data']


def BLPTSExample():
    result = BLPTS(['XS0316524130 Corp', 'US900123CG37 Corp'], ['PX_BID', 'INT_ACC', 'DAYS_TO_NEXT_COUPON'])
    result.get()
    print result.output
    result.closeSession()


#############################################################################


def main():
    pass

if __name__ == '__main__':
    main()
