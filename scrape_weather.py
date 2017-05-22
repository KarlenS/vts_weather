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
            try:
                cursor.execute(cmd)
                query_result = cursor.fetchall()
            except pymysql.err.ProgrammingError:
                raise SyntaxError('You fucked up...')

        return query_result


    def queryFIRInfo(self,date):

        cmd =  "SELECT * FROM `tblFIR_Pyrometer_Info` WHERE timestamp < ('%s' + INTERVAL 10 SECOND) AND timestamp > ('%s' - INTERVAL 10 SECOND)" %(date,date)
        fir_wthr = self.runQuery(cmd)
        return fir_wthr
    
    def queryWeatherSummary(self,date,time_window):
        cmd =  "SELECT * FROM `tblWeather_Status` WHERE timestamp < ('%s' + INTERVAL %d SECOND) AND timestamp > ('%s' - INTERVAL %d SECOND)" %(date,time_window,date,time_window)
        wthr_summary = self.runQuery(cmd)
        return wthr_summary

    def queryObserverWeather(self,date):
        cmd =  "SELECT weather  FROM `tblRun_Info` WHERE data_start_time - INTERVAL 30 MINUTE < '%s' AND data_end_time + INTERVAL 30 MINUTE > '%s'" %(date,date)
        obs_wthr = self.runQuery(cmd)
        return obs_wthr

    def getFIRInfo(self,date):
        return queryFIRInfo(self,date)

    def getWeatherSummary(self,date,time_window=30):

        query_result = self.queryWeatherSummary(date,time_window=time_window)
        wthsum_size = np.size(query_result)

        if wthsum_size == 1:
            return query_result
        elif wthsum_size == 0:
            query_result = self.queryWeatherSummary(date,time_window=time_window*2)
            wthsum_size = np.size(query_result)
            iters = 0
            while wthsum_size < 1 and iters <= 6:
                iters+=1
                time_window*=2
                query_result = self.queryWeatherSummary(date,time_window=time_window)
                wthsum_size = np.size(query_result)
            if iters > 6:
                return None
            else:
                return query_result
        else:
            query_result = self.queryWeatherSummary(date,time_window=time_window/2)
            wthsum_size = np.size(query_result)
            iters = 0
            while wthsum_size > 1 and iters <= 6:
                iters+=1
                time_window/=2
                query_result = self.queryWeatherSummary(date,time_window=time_window)
                wthsum_size = np.size(query_result)
            if iters > 6:
                return None
            else:
                return query_result
    
    def getObserverWeather(self,date):
        return queryObserverWeather(self,date)

def main():

    #mostly there, just need to have criteria for decent weather with FIRs and generate flags for each observation based on the three...
    filename = 'HESSJ1943_flux-r2/flux_photometry_r.dat'
    dat = np.genfromtxt(filename, usecols=(1,2,3,4,5),skip_header=1, names=['mjd','flux','fluxerr','refflux','reffluxerr'],unpack=True)

    dbr = DBReader()   

    dates = Time(dat['mjd'], format='mjd')
    for d in dates:
        #print  dbr.getObserverWeather(d.iso),d.iso
        #print dbr.getFIRInfo(d.iso), d.iso
        print dbr.getWeatherSummary(d.iso)

if __name__ == '__main__':
    main()
