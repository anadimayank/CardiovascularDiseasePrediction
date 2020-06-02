from flask import Flask, request, render_template,flash,redirect,send_file
from flask import Response
import os
import shutil
from flask_cors import CORS, cross_origin
from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.utils import secure_filename

from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction
UPLOAD_FOLDER='Uploads'
PREDICTION_FOLDER='Prediction_Output_File'
FileName=''
glbPath=''
ALLOWED_EXTENSIONS = set(['csv'])
os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
CORS(app)
def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def delete_allFilesinUploadsFolder():
    if os.path.exists(UPLOAD_FOLDER):
        shutil.rmtree(UPLOAD_FOLDER)
        print('removed folder ' + UPLOAD_FOLDER)
    if not os.path.exists(UPLOAD_FOLDER):
        os.mkdir(UPLOAD_FOLDER)
        print('created folder ' + UPLOAD_FOLDER)
    # filesToRemove = [os.path.join(UPLOAD_FOLDER,f) for f in os.listdir(UPLOAD_FOLDER)]
    # for f in filesToRemove:
    #     os.remove(f)
    #     print('removed file '+f)

def delete_allFilesinPredictionFolder():
    if os.path.exists(PREDICTION_FOLDER):
        shutil.rmtree(PREDICTION_FOLDER)
        print('removed folder ' + PREDICTION_FOLDER)
    if not os.path.exists(PREDICTION_FOLDER):
        os.mkdir(PREDICTION_FOLDER)
        print('created folder ' + PREDICTION_FOLDER)
    # filesToRemove = [os.path.join(PREDICTION_FOLDER,f) for f in os.listdir(PREDICTION_FOLDER)]
    # for f in filesToRemove:
    #     os.remove(f)
    #     print('removed file '+f)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    global glbPath
    glbPath=''
    global FileName
    FileName=''
    delete_allFilesinUploadsFolder()
    delete_allFilesinPredictionFolder()
    return render_template('index.html')

@app.route("/upload", methods=['GET'])
@cross_origin()
def uploadFileName():
    return render_template('index.html')

@app.route('/uploader', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            print('No file selected for uploading')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            delete_allFilesinUploadsFolder()
            filename = secure_filename(file.filename)
            my_path=os.path.join(app.config['UPLOAD_FOLDER'], filename)
            print(my_path)
            file.save(my_path)
            global FileName
            FileName=my_path
            print('File successfully uploaded')
            return redirect('/upload')
        else:
            print('Allowed file type is csv format only')
            return redirect(request.url)

@app.route('/return-files')
def return_files():
    global glbPath
    print("inside return file code"+glbPath)
    if glbPath!='':
        return send_file(glbPath, as_attachment=True, attachment_filename='Predictions.csv')
    else:
        return redirect('/')
@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        global glbPath
        print('Within predictRouteClient, glbpath:'+glbPath)

        if request.json is not None:
            path = request.json['filepath']

            pred_val = pred_validation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction(path)  # object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()
            glbPath=path
            return Response("Prediction File created at %s!!!" % path)
        elif request.form is not None:
            path = request.form['filepath']
            print(path)
            pred_val = pred_validation(path)  # object initialization
            print('Post Object initialization '+path)
            pred_val.prediction_validation()  # calling the prediction_validation function
            print('Post prediction_validation function ' + path)
            pred = prediction(path)  # object initialization
            print('Post prediction object initialization ' + path)
            # predicting for dataset present in database
            path = pred.predictionFromModel()
            glbPath = path
            print("Inside prediction code:"+path)
            return Response("Prediction File created at %s!!!" % path)

    except ValueError:
        print("Error Occurred! " + str(ValueError))
        return Response("Error Occurred! %s" % str(ValueError))
    except KeyError:
        print("Error Occurred! " + str(KeyError))
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        print("Error Occurred! " + str(e))
        return Response("Error Occurred! %s" % e)


@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            #rootProjPath = os.path.dirname(os.path.abspath(__file__))
            train_valObj = train_validation(path)  # object initialization

            train_valObj.train_validation()  # calling the training_validation function

            trainModelObj = trainModel()  # object initialization
            trainModelObj.trainingModel()  # training the model for the files in the table


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
