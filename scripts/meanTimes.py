import json
import time
import datetime as dt
import numpy as np
import pandas as pd

class MeanTimes:

    def meanTimeValues(rawData, dateRange):

        try:
            data = dataFormat(rawData, dateRange)
        except Exception as error:

            return {"error":True,"data":error,"status": 204}
        
        meanTimeValues = {"error":False,"data":data,"status": 200}
        return meanTimeValues


def meanTimeActivity(dataFrame):

    '''Itera en el dataFrame dado, identifica los intervalos de actividad entre estado activo gpio12 = 1
    e inactivo gpio12 = 0, suma total de las diferencias entre cada uno de los intervalos por su timestamp,
    timestemp donde gpio12 = 1 menos timestemp donde gpio12 = 0
    
    Parameters
    ----------
        dataFrame : Python Pandas DataFrame
            Dataframe que contiene samples de los estados del compresor.

    Returns
    -------
        int
            La suma total en unidad de segundos los intervalos de actividad.
    '''
    totalTime = 0
    i = 0
    while i < len(dataFrame):
        
        if(dataFrame.index[i] == len(dataFrame)-1 and dataFrame['gpio12'][dataFrame.index[i]] == 1 and dataFrame['chartParsed'][dataFrame.index[i]] == 1):
            lastActivity = str((dataFrame['timestamp'][dataFrame.index[i]])).split(" ")[1]
            dayend = "23:59:59"
            totalTime += (pd.Timedelta(pd.to_datetime(dayend, format='%H:%M:%S') - pd.to_datetime(lastActivity, format='%H:%M:%S')).seconds)
        
        elif(dataFrame['gpio12'][dataFrame.index[i]] == 1):
            try:
                totalTime += (pd.Timedelta(dataFrame['timestamp'][dataFrame.index[i+1]] - dataFrame['timestamp'][dataFrame.index[i]]).seconds)
            except IndexError:
                pass
        i += 1
    
    daysCount = dataFrame.timestamp.dt.dayofweek.value_counts().shape[0]
    meanTimeActivity = totalTime/daysCount
    return meanTimeActivity


def daysHrsMinSec(secondsResult):

    '''Toma una variable de tiempo en segundos y los convierte en días, horas, minutos y segundos.

    Parameters
    ----------
        secondsResult : int
            Los segudos totales de actividad del compresor.

    Returns
    -------
        list
            Una lista de enteros con el total de tiempo orden de días, horas, minutos y segundos.
    '''
    time = secondsResult
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    #'''str''' daysHrsMinSecStr =("%d días %d hr %d min %d seg" % (day, hour, minutes, seconds))
    daysHrsMinSecStr = [int(day), int(hour), int(minutes), int(seconds)]
    return daysHrsMinSecStr


def dataFormat(rawData, dateRange):

    '''Vacia los datos de promedio de actividad el compresor de los días de semana y/o fin de semana.

    Parameters
    ----------
        dataFrame1 : Python Pandas DataFrame
            DataFrame con los muestras de los días entre semana Lun-Vie.

        dataFrame2 : Python Pandas DataFrame
            DataFrame con los muestras de los días finales de semana Sab-Dom.

    Returns
    -------
        dict
            Un diccionario con el total de tiempo orden de días, horas, minutos y segundos
            correspondiente a días de semana y fines de semana.
    '''
    days = 5
    dFTimes = pd.DataFrame(rawData['data'])
    dFTimes['timestamp'] = pd.to_datetime(dFTimes.timestamp, format='%Y-%m-%d %H:%M:%S')
    dFWeek = dFTimes.loc[dFTimes.timestamp.dt.dayofweek < days].reset_index(drop=True)
    dFWknd = dFTimes.loc[dFTimes.timestamp.dt.dayofweek >= days].reset_index(drop=True)
    inactivityDays = inactvityDays(dFTimes)
    cSdates = containsSundayDate(dateRange)

    if(cSdates):
        for i in cSdates:
            bd = pd.to_datetime("{0} 00:00:00".format(i), format='%Y-%m-%d %H:%M:%S')
            ed = pd.to_datetime("{0} 23:59:59".format(i), format='%Y-%m-%d %H:%M:%S')
            data = {'timestamp':[bd, bd, ed, ed],
                    'gpio12':[1, 1, 1, 1],
                    'chartParsed':[1, 1, 1, 1]}
            Sunday = pd.DataFrame(data,columns = ['timestamp', 'gpio12', 'chartParsed'])
            dFWknd = dFWknd.append(Sunday, ignore_index = True)

    data = {}
    Zero = { "days": 0, "hours": 0, "min": 0, "sec": 0 }

    if((not dFWeek.empty) and (not dFWknd.empty)):

        mTA = meanTimeActivity(dFWeek)
        dHMS = daysHrsMinSec(mTA)
        mTAWknd = meanTimeActivity(dFWknd)
        dHMSWknd = daysHrsMinSec(mTAWknd)

        data = {
            "weekday": { "days": dHMS[0], "hours": dHMS[1], "min": dHMS[2], "sec": dHMS[3] },
            "weekend": { "days": dHMSWknd[0], "hours": dHMSWknd[1], "min": dHMSWknd[2], "sec": dHMSWknd[3] },
            "comp_off": {"days": inactivityDays}
        }
    elif((not dFWeek.empty) and (dFWknd.empty)):

        mTA = meanTimeActivity(dFWeek)
        dHMS = daysHrsMinSec(mTA)

        data = {
            "weekday": { "days": dHMS[0], "hours": dHMS[1], "min": dHMS[2], "sec": dHMS[3] },
            "weekend": Zero,
            "comp_off": {"days": inactivityDays}
        }
    elif((dFWeek.empty) and (not dFWknd.empty)):

        mTAWknd = meanTimeActivity(dFWknd)
        dHMSWknd = daysHrsMinSec(mTAWknd)

        data = {
            "weekday": Zero,
            "weekend": { "days": dHMSWknd[0], "hours": dHMSWknd[1], "min": dHMSWknd[2], "sec": dHMSWknd[3] },
            "comp_off": {"days": inactivityDays}
        }
    elif((dFWeek.empty) and (dFWknd.empty)):
        
        data = {
            "weekday": Zero,
            "weekend": Zero,
            "comp_off": {"days": inactivityDays}
        }
    else:
        data = {
            "Error": "Error"
        }
    return data


def inactvityDays(dataFrame):

    '''Identifica los días sin actividad del compresor.

    Parameters
    ----------
        dataFrame : Python Pandas DataFrame
            Todas las muestras solicitadas.

    Returns
    -------
        int
            el total de días sin actividad.
    '''
    dataFrame = dataFrame.groupby(dataFrame.timestamp.dt.date).mean()
    dataFrame = dataFrame.loc[dataFrame.gpio12 == 0].reset_index(drop=True)
    if(not dataFrame.empty):
        inactvityDays = len(dataFrame)
    else:
        inactvityDays = 0

    return inactvityDays


def containsSundayDate(dateRange):
    '''
    Auxilar con los días de actividad completa
    '''
    dateBegin = str(dt.datetime.fromtimestamp(dateRange[0]).strftime("%Y-%m-%d")).split("-")
    dateEnd = str(dt.datetime.fromtimestamp(dateRange[1]).strftime("%Y-%m-%d")).split("-")

    dateBegin = dt.datetime(int(dateBegin[0]), int(dateBegin[1]), int(dateBegin[2]))
    dateEnd = dt.datetime(int(dateEnd[0]), int(dateEnd[1]), int(dateEnd[2]))
    deltaTime = dateEnd - dateBegin
    cSdates = []
    for i in range(deltaTime.days + 1):
        day = dateBegin + dt.timedelta(days = i)
        if (dt.datetime.weekday(day) == 6):
            item = pd.to_datetime(day, format='%Y-%m-%d')
            item = str(item).split(" ")[0]
            cSdates.append(item)
    return cSdates
