from subprocess import Popen, PIPE, STDOUT
import os

triageDir = "\\\\romeo\\Collect\\spe\\Greg\\ua200_testing"
#createSIP
print "bagging accession"
if os.name == "nt":
	sipCmd = "python C:\\Projects\\createsip\\createsip.py " + os.path.join(triageDir, "Requests")
else:
	sipCmd = "sudo python /home/bcadmin/Projects/createSIP/createSIP.py " + os.path.join(triageDir, "Requests")
createSIP = Popen(sipCmd, stdout=PIPE, stdin=PIPE, stderr=STDOUT)
grep_stdout = createSIP.communicate("ua200\nElisa Lopez\nRecords from the University Senate\n\nua200.py crawler\n\nUniversity Senate\nSecretary\nemlopez@albany.edu\nUNH 302\n\n\n\n\nY\n")
print grep_stdout