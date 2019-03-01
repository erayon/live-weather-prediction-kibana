import datetime
import time
import sys
import io
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import pandas as pd
from dateutil import parser, rrule
from datetime import datetime, time, date


class HistorialWeatherData:

    def getRainfallData(self, station, day, month, year):
        """
        Function to return a data frame of minute-level weather data for a single Wunderground PWS station.
        
        Args:
            station (string): Station code from the Wunderground website
            day (int): Day of month for which data is requested
            month (int): Month for which data is requested
            year (int): Year for which data is requested
        
        Returns: IDELHINE8
            Pandas Dataframe with weather data for specified station and date.

        """
        url = "http://www.wunderground.com/weatherstation/WXDailyHistory.asp?ID={station}&day={day}&month={month}&year={year}&graphspan=day&format=1"
        full_url = url.format(station=station, day=day, month=month, year=year)
        # Request data from wunderground data
        response = requests.get(full_url) #headers={'User-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
        data = response.text
        # remove the excess <br> from the text data
        data = data.replace('<br>', '')
        #print(data)
        # Convert to pandas dataframe (fails if issues with weather station)
        try:
            dataframe = pd.read_csv(io.StringIO(data), index_col=False)
            dataframe['station'] = station
        except Exception as e:
            print("Issue with date: {}-{}-{} for station {}".format(day,month,year, station))
            return None
        return dataframe

    def HistoricalData(self,stations,start_date,end_date):
        """
        Define stations or list of stations : 
            e.i : stations = ["IDUBLINF3", "IDUBLINF2", "ICARRAIG2", "IGALWAYR2", "IBELFAST4", "ILONDON59", "IILEDEFR28"]
        start_date = "2015-01-01"
        end_date = "2015-12-31"
        Finally combine all of the individual days and output to CSV for analysis
        '''
        pd.concat(data[station]).to_csv("data/{}_weather.csv".format(station))

        '''
        """
        start_date = start_date
        end_date = end_date
        start = parser.parse(start_date)
        end = parser.parse(end_date)
        dates = list(rrule.rrule(rrule.DAILY, dtstart=start, until=end))

        # Create a list of stations here to download data for
        # Set a backoff time in seconds if a request fails
        backoff_time = 10
        data = {}

        # Gather data for each station in turn and save to CSV.
        for station in stations:
            print("Working on {}".format(station))
            data[station] = []
            for date in tqdm(dates):
                # Print period status update messages
                if date.day % 10 == 0:
                    #print("Working on date: {} for station {}".format(date, station))
                    a = 0
                done = False
                while done == False:
                    try:
                        weather_data = self.getRainfallData(station, date.day, date.month, date.year)
                        #print(weather_data)
                        done = True
                    except ConnectionError as e:
                        # May get rate limited by Wunderground.com, backoff if so.
                        print("Got connection error on {}".format(date))
                        print("Will retry in {} seconds".format(backoff_time))
                        time.sleep(10)
                # Add each processed date to the overall data
                data[station].append(weather_data)
            # Finally combine all of the individual days and output to CSV for analysis.
            pd.concat(data[station]).to_pickle("{}_weather_full_data.pckl".format(station))
        data_raw = pd.read_pickle('{}_weather_full_data.pckl'.format(station))
        # Give the variables some friendlier names and convert types as necessary.
        data_raw['temp'] = data_raw['TemperatureC'].astype(float)
        data_raw['rain'] = data_raw['HourlyPrecipMM'].astype(float)
        data_raw['total_rain'] = data_raw['dailyrainMM'].astype(float)
        data_raw['date'] = data_raw['DateUTC'].apply(parser.parse)
        data_raw['humidity'] = data_raw['Humidity'].astype(float)
        data_raw['Dewpoint'] = data_raw['DewpointC'].astype(float)
        data_raw['wind'] = data_raw['WindSpeedKMH']
        data = data_raw.loc[:, ['date', 'station', 'temp', 'rain', 'total_rain', 'humidity','Dewpoint']]
        return data
        #data.to_pickle('{}_weather_data.pckl'.format(station))


