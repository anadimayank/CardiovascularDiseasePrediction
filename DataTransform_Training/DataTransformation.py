from datetime import datetime
from os import listdir
from application_logging.logger import App_Logger
import pandas as pd
import os

class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

     def __init__(self):
          #THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
          #my_file = os.path.join(rootProjPath, 'Training_Raw_files_validated\\Good_Raw')
          #self.goodDataPath = my_file
          self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
          self.logger = App_Logger()
          #self.rootProjPath=rootProjPath


     def addQuotesToStringValuesInColumn(self):
          """
                                           Method Name: addQuotesToStringValuesInColumn
                                           Description: This method converts all the columns with string datatype such that
                                                       each value for that column is enclosed in quotes. This is done
                                                       to avoid the error while inserting string values in table as varchar.

                                            Written By: iNeuron Intelligence
                                           Version: 1.0
                                           Revisions: None

                                                   """
          #THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
          #my_file = rootProjPath+'\\'+'Training_Logs\\addQuotesToStringValuesInColumn.txt'
          #log_file = open(my_file, 'a+')
          log_file = open("Training_Logs/addQuotesToStringValuesInColumn.txt", 'a+')
          try:
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    data = pd.read_csv(self.goodDataPath+"/" + file)
                    #list of columns with string datatype variables
                    column = ['id','age','gender','height','weight','ap_hi','ap_lo','cholesterol','gluc','smoke','alco','active','cardio']

                    for col in data.columns:
                         if col in column: # add quotes in string value
                              data[col] = data[col].apply(lambda x: "'" + str(x) + "'")
                         if col not in column: # add quotes to '?' values in integer/float columns
                              data[col] = data[col].replace('?', "'?'")
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    #csv['Wafer'] = csv['Wafer'].str[6:]
                    data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.logger.log(log_file," %s: Quotes added successfully!!" % file)
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
          except Exception as e:
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               log_file.close()
          log_file.close()
