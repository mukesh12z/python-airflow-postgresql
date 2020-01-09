import os
import json
import numpy as np
import pandas as pd
import unittest

def check_return(df):
   
    '''
    Checks if starting and ending airport is same and 'OneWayOrReturn' is
    'Return' then return  and vice versa

    Note: i have changed code a little (iloc instead of column name as passing
    json directly instead of file)
    ''' 
  
# If Reason is already set for Itinerary then no need to check for return validity  

    if (str(df.iloc[0,0]).split('-')[0] != str(df.iloc[0,0]).split('-')[-1]):
        if str(df.iloc[0,1]) == 'Return':
            return 'OneWayOrReturn'
    else:
        if str(df.iloc[0,1]) != 'Return':
            return 'OneWayOrReturn'
         
def check_itinerary(df):
   
    '''
    Checks if starting and ending airport is not same and 'OneWayOrReturn' is
    'Return' then error
    ''' 
    
    if(str(df.iloc[0,1]).startswith(str(df.iloc[0,0]))):
        pass
    else:
        return 'Itinerary'

class TestTrip(unittest.TestCase):
    
    def test_check_return_pos(self):
        df =  pd.DataFrame([{'Itinerary':'ABC-DEF-ABC','OneWayOrReturn':'Return'}])
        assert check_return(df) == None  
    
    def test_check_return_neg(self):
        df = pd.DataFrame([{'Itinerary': 'ABC-DEF-ABC-GHI','OneWayOrReturn':'Return'}])
        assert check_return(df) == 'OneWayOrReturn'
  
    def test_check_itinerary_pos(self):
        df = pd.DataFrame([{'Itinerary': 'ABC-DEF-ABC','DepartureAirport':'ABC-DEF'}])
        assert check_itinerary(df) == None 
    
    def test_check_itinerary_neg(self):
        df = pd.DataFrame([{'Itinerary': 'ABC-DEF-ABC','DepartureAirport':'ABC-CDF'}])
        assert check_itinerary(df) == 'Itinerary' 
       
if __name__ == "__main__":
    unittest.main()
