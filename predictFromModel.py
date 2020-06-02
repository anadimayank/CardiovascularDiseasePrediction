import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
import pickle


class prediction:

    def __init__(self,path):
        #self.file_object = open(rootProjPath+"\\Prediction_Logs\\Prediction_Log.txt", 'a+')
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.pred_data_val = Prediction_Data_validation(path)
        self.path=path

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.log_writer.log(self.file_object,'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data=data_getter.get_data()

            #code change
            # wafer_names=data['Wafer']
            # data=data.drop(labels=['Wafer'],axis=1)

            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            data = preprocessor.dropUnnecessaryColumns(data,['smoke', 'alco'])

            # replacing '?' values with np.nan as discussed in the EDA part

            data = preprocessor.replaceInvalidValuesWithNull(data)

            # get encoded values for categorical data

            #data = preprocessor.encodeCategoricalValuesPrediction(data)
            is_null_present=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            #data=data.to_numpy()
            file_loader=file_methods.File_Operation(self.file_object,self.log_writer)
            kmeans=file_loader.load_model('KMeans')

            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(data)#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            result=[] # initialize balnk list for storing predicitons
            #with open(self.rootProjPath+'\\EncoderPickle\\enc.pickle', 'rb') as file: #let's load the encoder pickle file to decode the values
            #    encoder = pickle.load(file)

            for i in clusters:
                cluster_data= data[data['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                #encoder.inverse_transform(model.predict(cluster_data))
                for val in (model.predict(cluster_data)):
                    result.append(val)
            result = pandas.DataFrame(result,columns=['Predictions'])
            print(self.path)
            if (self.path.find('Prediction_Batch_files') != -1):
                mypath = "Prediction_Output_File/Predictions.csv"
            else:
                mypath = "Uploads/Predictions.csv"
            print(mypath)
			#path = "Prediction_Output_File/Predictions.csv"
            result.to_csv(mypath, header=True,) #appends result to prediction file
            self.log_writer.log(self.file_object,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return mypath