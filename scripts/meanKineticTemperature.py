import numpy as np
from queries.thermistor_query import ThermistorQuery

class MeanKineticTemperature:

    def mktvalues(rawSamples, device):

        try:
            data = dataFormat(rawSamples, device)
        except Exception as error:
            return {"error":True,"data":error,"status": 204}
            
        mkt = {"error":False,"data":data,"status": 200}
        return mkt


def mktAlgorithm(SampletempsArray, DeltaH, R):
    ''' Formula:

        -ΔH/R / (ln(exp{-(ΔH/R)/(T1+273.15)} + ... + exp{-(ΔH/R)/(Tn+273.15)}))

        ΔH es Heat of activation o activation energy
        "A value of around 83 is considered typical for pharmaceuticals or foodstuffs."
        Valores Utilizados en Ejemplos entre 83100 y 85000
        la constante universal del gas al siguiente valor, Gas Constant: 8.31446261815324.

        Relación Kevin, Celsius K° = C° + 273.15

        Valores de ejemplo base:
        ΔH = 85000
        R = 8.31446261815324

        Valores utilizados en otros ejemplos:
        ΔH = 83.14472 kJ/mol
        R = 0.008314472 kJ/mol'''

    Kelvin = 273.15

    DHdivR = (DeltaH / R)
    Sigma = 0

    for x in SampletempsArray:
        Sigma += np.exp(-(DHdivR / (x + Kelvin)))

    lnSigma = np.log(Sigma / len(SampletempsArray))

    mktK = - DHdivR / lnSigma

    mktC = mktK - Kelvin

    return  mktC#, mktK

def dataFormat(rawSamples, device):

    constVal = ThermistorQuery.getMktAConsts(device)
    if not constVal['err'] and constVal['data']:    
        DeltaH = constVal['data'][0][0]
        R = constVal['data'][0][1]
    else:
        DeltaH = 83.14472
        R = 0.008314472

    SamplesTemp1 = np.array([temp['temp1'] for temp in rawSamples['data']])
    SamplesTemp2 = np.array([temp['temp2'] for temp in rawSamples['data']])
    SamplesTemp3 = np.array([temp['temp3'] for temp in rawSamples['data']])
    SamplesTemp4 = np.array([temp['temp4'] for temp in rawSamples['data']])

    mktT1 = mktAlgorithm(SamplesTemp1, DeltaH, R)
    mktT1 = mktAlgorithm(SamplesTemp2, DeltaH, R)
    mktT3 = mktAlgorithm(SamplesTemp3, DeltaH, R)
    mktT4 = mktAlgorithm(SamplesTemp4, DeltaH, R)

    data = {
            "mkt1": mktT1,
            "mkt2": mktT1,
            "mkt3": mktT3,
            "mkt4": mktT4
            }
    return data
