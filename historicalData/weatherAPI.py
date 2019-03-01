import pandas as pd 
import numpy as np 
from tqdm import tqdm,tqdm_notebook
from os import path
import os
import sys
import datetime
import time
from sklearn.impute import SimpleImputer
sys.path.append(path.abspath(os.getcwd()))
from get_historical_data import HistorialWeatherData
hw = HistorialWeatherData()


class WeatherAPI:

	def __init__(self):
		self.start_time   = "06:00:00"
		self.end_time     = "20:00:00"
		self.sample_delay = "5T"
		self.SaveFolder   = None

	def create_pckl(self,station_id,start_date,end_date,start_time="06:00:00",end_time="20:00:00",sample_delay="5T",SaveFolder=None):
		'''
		Create the pickle file's of the weather 

		:type station_id: string
		:param station_id: Station code from the Wunderground website [ex:'IDELHINE8']

		:type start_date: string
		:param start_date: start date [ex: "2018-06-01"]

		:type end_date: string
		:param end_date: end date [ex: "2018-06-30"]

		:type start_time: string
		:param start_time: start time [default: "06:00:00"]

		:type end_time: string
		:param end_time: end time [default: "20:00:00"]

		:type sample_delay: string
		:param sample_delay: sample delay in Mintues [default: "5T"]

		'''
		self.sample_delay = sample_delay
		self.start_time   = start_time
		self.end_time     = end_time
		self.SaveFolder   = SaveFolder
		HD = hw.HistoricalData([station_id],start_date,end_date)

		def datetimeConv(ts):
			x, y = str(ts).split(" ")
			return str(x)

		def fun2(ts):
			x, y = str(ts).split(" ")
			return str(y)

		def fun3(ts):
			x, y , z = str(ts).split(":")
			return str(x)

		def createFolder(name=None):
			if name == None:
				genPath ='./weather_pickle/pickle_data/'
				if not os.path.exists(genPath):
					os.makedirs(genPath)
				return genPath
			elif name:
				genPath = './weather_pickle/pickle_data/'+name+'/'
				if not os.path.exists(genPath):
					os.makedirs(genPath)
				return genPath

		def dataframeconvert(args,colname):
			tt = pd.DataFrame(args)
			tt['timestamp'] = tt.index
			tt = tt.reset_index(drop=True)
			tt.columns = [colname,'timestamp']
			tt = tt[['timestamp',colname]]
			tt['time'] = tt.apply(lambda row: fun2(row['timestamp']), axis=1)
			print(tt)
			df = tt[(tt['time'] >= self.start_time) & (tt['time'] <= self.end_time)].reset_index(drop=True)
			imp = SimpleImputer(missing_values='NaN', strategy='mean')
			imp.fit(np.array(df[colname]).reshape(-1,1))
			val = imp.transform(np.array(df[colname]).reshape(-1,1))
			df[colname] = val
			return df

		HD['utc_dt'] = HD.apply(lambda row: datetimeConv(row['date']), axis=1)
		if SaveFolder == None:
			genPath = createFolder()
		elif SaveFolder:
			genPath = createFolder(station_id)
		for every_date in tqdm_notebook(HD.utc_dt.unique()):
			try:
				sample_HD                       		 = HD[HD['utc_dt']==every_date]
				timestamp                       		 = pd.to_datetime(sample_HD['date'] )
				outside_temp                    		 = sample_HD['temp'].astype('float')
				outside_humidity                		 = sample_HD['humidity'].astype('float')
				outside_Dewpoint          		         = sample_HD['Dewpoint'].astype('float')
				outside_temp_series             		 = pd.Series(np.array(outside_temp), index=timestamp)
				outside_temp_series_resample    		 = outside_temp_series.resample(self.sample_delay,label="right").mean()
				outside_humidity_series          		 = pd.Series(np.array(outside_humidity), index=timestamp)
				outside_humidity_series_resample 		 = outside_humidity_series.resample(self.sample_delay,label="right").mean()
				outside_Dewpoint_series	 		         = pd.Series(np.array(outside_Dewpoint), index=timestamp)
				outside_Dewpoint_series_resample         = outside_Dewpoint_series.resample(self.sample_delay,label="right").mean()
				#temp_tempDf                    			 = dataframeconvert(outside_temp_series_resample,'Temperature')
				temp_humidity                   		 = dataframeconvert(outside_humidity_series_resample,'Humidity')
				#temp_dewpoint                   		 = dataframeconvert(outside_Dewpoint_series_resample,'Dewpoint')
				#temp_tempDf.to_pickle(genPath+'/{}_temperature.pckl'.format(every_date))
				#temp_humidity.to_pickle(genPath+'/{}_humidity.pckl'.format(every_date))
				#temp_dewpoint.to_pickle(genPath+'/{}_dewpoint.pckl'.format(every_date))
			except Exception as e:
				print(e)

	def UniqueDates(self):
		'''
		Return Unique Dates

		'''
		directory = './weather/pickle_data/'
		DEBUG = False
		l = []
		for file in os.listdir(directory):
			img = directory + file
			if DEBUG: print(img)
			l.append(img)
		l = sorted(l)
		dates = []
		for eveyitem in l:
			dates.append(eveyitem.split('/')[3].split('_')[0])
		return np.unique(dates)



if __name__ == "__main__":
	obj = WeatherAPI()
	obj.create_pckl(
		        station_id   = 'IKOLKATA3',
				start_date   = datetime.date.today().strftime('%Y-%m-%d'),
				end_date     = datetime.date.today().strftime('%Y-%m-%d'),
				start_time   = (datetime.datetime.now() - datetime.timedelta(minutes=120)).strftime('%H:%M:%S'),
				end_time     = time.strftime('%H:%M:%S'),
				sample_delay = '5T',
				SaveFolder   = 'IKOLKATA3'
				) 