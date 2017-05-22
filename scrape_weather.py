import numpy as np
import pymysql.cursors
from datetime import date, datetime


class DBReader(object):


    def __init__(self,db='VERITAS'):
        '''Initiates connection to a database. With current setup, each objects connects to a single database.
        This means, a separate object is required for VERITAS and VOFFLINE databases.
        '''
        self.client = pymysql.connect(host='lucifer1.spa.umn.edu',
                                     user='readonly',
                                     port=33060,
                                     db=db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)


    def runQuery(self, cmd):
        '''runs the mysql command provided and returns the output list of results'''
    
        with self.client.cursor() as cursor:
            cursor.execute(cmd)
        query_result = cursor.fetchall()

        return query_result


    def getFIRInfo(self):
        return fir_wthr

    def getObserverWeather(self):
        return obs_wthr

    def getWeatherSummary(self):
        return wthr_summary

def main():
    filename = 'HESSJ1943_Rerun/data-1/rep_photometry_r.dat'
    dat = np.genfromtxt(filename, usecols=(1,2,3,4,5), names=['mjd','mag','refmag','magerr','refmagerr'],unpack=True)
    

if __name__ == '__main__':
    main()
