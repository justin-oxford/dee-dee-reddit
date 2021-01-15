import databaseConn
import deedee_ML_dataPull
import deedee_ML_main
import deedee_ML_testModel
import PAPER
import datetime
import time

#
while True:
    selection = input("[1] Run Machine Learning\n[2] Run Machine Prediction\n[3] Run PAPER\n[0] CLOSE\n\nSelection: ")
    if selection == '0':
        break
    elif selection == '1':
        deedee_ML_main.main()
    elif selection == '2':
        # run it once
        deedee_ML_testModel.deedee_predict_main()

        while True:
            current_time = datetime.datetime.now()
            while current_time.minute % 30 != 0:
                print("Next Pull in " + str(30 - (current_time.minute % 30)) + " minutes.")
                time.sleep(60)
                current_time = datetime.datetime.now()
            print("Generating Insight...")
            deedee_ML_testModel.deedee_predict_main()
    elif selection == '3':
        print("PAPER is not active.")
