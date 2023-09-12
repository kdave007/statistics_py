from collections import OrderedDict
import ciso8601 as ciso
import numpy as np
import time

class ramerDouglas:
    def rdpValues(rawSamples):
        try:
            data = dataFormat(rawSamples)
        except Exception as error:
            return {"error":True,"data":error,"status": 204}
            
        rdp = {"error":False,"data":data,"status": 200}
        return rdp

def ramerDouglasPeucker(SampleTempsArray, Epsilon):
    start = np.tile(np.expand_dims(SampleTempsArray[0], axis=0), (SampleTempsArray.shape[0], 1))
    end = np.tile(np.expand_dims(SampleTempsArray[-1], axis=0), (SampleTempsArray.shape[0], 1))

    d = np.abs(np.cross(end - start, SampleTempsArray - start, axis=-1)) / np.linalg.norm(end - start, axis=-1)
    max = np.argmax(d)
    dmax = d[max]

    resultRDP = []
    if dmax > Epsilon:
        left = ramerDouglasPeucker(SampleTempsArray[:max + 1], Epsilon)
        resultRDP += [list(x) for x in left if list(x) not in resultRDP]
        right = ramerDouglasPeucker(SampleTempsArray[max:], Epsilon)
        resultRDP += [list(x) for x in right if list(x) not in resultRDP]
    else:
        resultRDP += [SampleTempsArray[0], SampleTempsArray[-1]]

    return resultRDP

def reconstructor(tmpList1, tmpList2, tmpList3, tmpList4):
    tmpList1 = [x + [None, None, None] for x in tmpList1]
    snd(tmpList1, tmpList2, 2)
    snd(tmpList1, tmpList3, 3)
    snd(tmpList1, tmpList4, 4)

    tmpList2 = [x + [None, None, None] for x in tmpList2]
    reorder(tmpList2, 2)
    snd(tmpList2, tmpList3, 3)
    snd(tmpList2, tmpList4, 4)

    tmpList3 = [x + [None, None, None] for x in tmpList3]
    reorder(tmpList3, 3)
    snd(tmpList3, tmpList4, 4)

    tmpList4 = [x + [None, None, None] for x in tmpList4]
    reorder(tmpList4, 4)
    
    return sort(tmpList1, tmpList2, tmpList3, tmpList4)

def snd(tmpList1, tmpList2, position):
    for item1 in tmpList1:
        for item2 in tmpList2:
            if(item1[0] == item2[0]):
                item1[position] = item2[1]
                tmpList2.remove(item2)

def reorder(tmpList, position):
    for item in tmpList:
        item[position] = item[1]
        item[1] = None

def sort(tmpList1, tmpList2, tmpList3, tmpList4):
    tmpList = tmpList1 + tmpList2 + tmpList3 + tmpList4
    tmpList = sorted(tmpList,key=lambda x: x[0])
    return tmpList

def dateTimeIndex(date):
    date = ciso.parse_datetime(date)
    date = time.mktime(date.timetuple())
    return date

def indexToDateTime(date):
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(date))
    return date

def jsonFormat(rdpList):
    jsonlist = []
    for x in rdpList:
        dict = OrderedDict()
        dict["dateTime"] = indexToDateTime(x[0])
        dict["temp1"] = x[1]
        dict["temp2"] = x[2]
        dict["temp3"] = x[3]
        dict["temp4"] = x[4]
        jsonlist.append(dict)
    return jsonlist

def dataFormat(rawSamples):
    SamplesTemp1 = np.array([[ dateTimeIndex(x['dateTime']), x['temp1'] ] for x in rawSamples['data']])
    SamplesTemp2 = np.array([[ dateTimeIndex(x['dateTime']), x['temp2'] ] for x in rawSamples['data']])
    SamplesTemp3 = np.array([[ dateTimeIndex(x['dateTime']), x['temp3'] ] for x in rawSamples['data']])
    SamplesTemp4 = np.array([[ dateTimeIndex(x['dateTime']), x['temp4'] ] for x in rawSamples['data']])

    tmpList1 = ramerDouglasPeucker(SamplesTemp1, 0.5)
    tmpList2 = ramerDouglasPeucker(SamplesTemp2, 0.5)
    tmpList3 = ramerDouglasPeucker(SamplesTemp3, 0.5)
    tmpList4 = ramerDouglasPeucker(SamplesTemp4, 0.5)

    tmpJson = jsonFormat(reconstructor(tmpList1, tmpList2, tmpList3, tmpList4))

    return tmpJson
