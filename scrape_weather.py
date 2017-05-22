import numpy as np
import pymysql.cursors
from astropy.time import Time


class DBReader(object):


    def __init__(self,db='VERITAS'):
        '''Initiates connection to a database. With current setup, each objects connects to a single database.
        This means, a separate object is required for VERITAS and VOFFLINE databases. Only have VERITAS anyways.
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


    def getFIRInfo(self,date):

        cmd =  "SELECT * FROM `tblFIR_Pyrometer_Info` ORDER BY ABS( DATEDIFF( timestamp, %s ) ) LIMIT 1" %(date)
        fir_wthr = self.runQuery(cmd)
        return fir_wthr

    def getObserverWeather(self):
        obs_withr = 0
        return obs_wthr

    def getWeatherSummary(self):
        wthr_summary = 0
        return wthr_summary

def main():
    filename = 'HESSJ1943_flux-r2/flux_photometry_r.dat'
    dat = np.genfromtxt(filename, usecols=(1,2,3,4,5),skip_header=1, names=['mjd','flux','fluxerr','refflux','reffluxerr'],unpack=True)

    dbr = DBReader()   

    t = Time(55197., format='mjd')
    dbr.getFIRInfo(t.iso)
    

if __name__ == '__main__':
    main()
