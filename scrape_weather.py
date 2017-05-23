import numpy as np
import pymysql.cursors
from astropy.time import Time


def getRMS(vals):
    if np.size(vals) != 0:
        return np.sqrt(np.mean(np.square(vals)))
    else:
        return 0

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

        cmd =  "SELECT * FROM `tblFIR_Pyrometer_Info` WHERE timestamp < ('%s' + INTERVAL 15 MINUTE) AND timestamp > ('%s' - INTERVAL 15 MINUTE)" %(date,date)
        fir_wthr = self.runQuery(cmd)
        return fir_wthr
    
    def queryWeatherSummary(self,date,time_window):
        cmd =  "SELECT * FROM `tblWeather_Status` WHERE timestamp < ('%s' + INTERVAL %d SECOND) AND timestamp > ('%s' - INTERVAL %d SECOND)" %(date,time_window,date,time_window)
        wthr_summary = self.runQuery(cmd)
        return wthr_summary

    def queryObserverWeather(self,date):
        cmd =  "SELECT weather  FROM `tblRun_Info` WHERE data_start_time - INTERVAL 20 MINUTE < '%s' AND data_end_time + INTERVAL 20 MINUTE > '%s'" %(date,date)
        obs_wthr = self.runQuery(cmd)
        return obs_wthr

    def getFIRInfo(self,date):
        

        query_result = self.queryFIRInfo(date)
        if np.size(query_result) == 0:
            return -1
        else:
            sky_temp_t0 = np.zeros(0)
            sky_temp_t1 = np.zeros(0)
            sky_temp_t3 = np.zeros(0)
            time_t0 = np.zeros(0)
            time_t1 = np.zeros(0)
            time_t3 = np.zeros(0)

            for count,item in enumerate(query_result):
                if item['telescope_id'] == 0:
                    sky_temp_t0 = np.append(sky_temp_t0,item['radiant_sky_temp'])
                    time_t0 = np.append(time_t0,item['timestamp'])
                elif item['telescope_id'] == 1 or item['telescope_id'] == 2:
                    sky_temp_t1 = np.append(sky_temp_t1,item['radiant_sky_temp'])
                    time_t1 = np.append(time_t1,item['timestamp'])
                elif item['telescope_id'] == 3:
                    sky_temp_t3 =np.append(sky_temp_t3,item['radiant_sky_temp'])
                    time_t3 = np.append(time_t3,item['timestamp'])
                else:
                    raise ValueError('Well shit. Not a telescope.')

            rms_t0 = getRMS(sky_temp_t0)
            rms_t1 = getRMS(sky_temp_t1)
            rms_t3 = getRMS(sky_temp_t3)
            '''
            import matplotlib.pyplot as plt
            import seaborn as sns
            sns.set_style('ticks')
            fig,[ax1,ax2,ax3] = plt.subplots(3, figsize=(11, 8), dpi=75)
            ax1.plot(time_t0,sky_temp_t0,color='black')
            ax1.fill_between(time_t0,np.mean(sky_temp_t0)*0.9,np.mean(sky_temp_t0)*1.1,color = 'grey',alpha=0.5)
            ax2.plot(time_t1,sky_temp_t1,color='red')
            ax2.fill_between(time_t0,np.mean(sky_temp_t1)*0.9,np.mean(sky_temp_t1)*1.1,color = 'red',alpha=0.5)
            ax3.plot(time_t3,sky_temp_t3,color='green')
            ax3.fill_between(time_t0,np.mean(sky_temp_t3)*0.9,np.mean(sky_temp_t3)*1.1,color = 'green',alpha=0.5)
            plt.show()
            '''
            return 1

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
                return [{'weather': None}]
            else:
                return query_result
    
    def getObserverWeatherFlag(self,date):

        good_weather = ['A','A+','A-','B+','B','B-']

        query_result = self.queryObserverWeather(date)
        obswthr_size = np.size(query_result)

        if obswthr_size == 0:
            return -1
        elif obswthr_size == 1:
            if query_result[0]['weather'] in good_weather:
                return 1
            else:
                return 0
        else: 
            for item in query_result:
                if item['weather'] not in good_weather: 
                    return 0
            else:
                return 1

def main():

    #mostly there, just need to have criteria for decent weather with FIRs and generate flags for each observation based on the three...
    filename = 'HESSJ1943_flux-r2/flux_photometry_r.dat'
    dat = np.genfromtxt(filename, usecols=(1,2,3,4,5),skip_header=1, names=['mjd','flux','fluxerr','refflux','reffluxerr'],unpack=True)

    dbr = DBReader()   
    obs_flags = np.zeros_like(dat['mjd'],dtype=np.int)
    fir_flags = np.zeros_like(dat['mjd'],dtype=np.int)
    sum_flags = np.zeros_like(dat['mjd'],dtype=np.int)

    dates = Time(dat['mjd'], format='mjd')

    for count,d in enumerate(dates):
        #Checks every date for observer weather assessment and makes flags for good/bad/missing (1,0,-1)
        obs_flags[count] = dbr.getObserverWeatherFlag(d.iso)

        #print dbr.getFIRInfo(d.iso), d.iso
        #print dbr.getWeatherSummary(d.iso)


if __name__ == '__main__':
    main()
