#!/bin/bash
set -x
users=$1
workflowId=$2
errorMsg=$3
logDetails=$4
hostName=$5
failureLog=/userapps/hadoop/ace/log/AceJobFailure.log

error_msg="$cur_date ACE job failed. Please check the workflow $workflowId and the log http://d-3zktk02.target.com:19888/jobhistory/job/$logDetails" ;

echo $error_msg;

ssh -oStrictHostKeyChecking=no -t $hostName "test \"\$(cat $failureLog|wc -l)\" -gt 30 && rm -f $failureLog; echo \"$error_msg\" >> $failureLog";

(
echo "From: Hadoop.Admin@target.com"
echo "To: $users"
echo "MIME-Version: 1.0"
echo "Subject: ACE JOB FAILURE $workflowId"
echo "Hi All,
        Today's ACE Job has failed. Find the details below.
        Error message is: $errorMsg
        Oozie job id: $workflowId
        Job log location is: http://d-3zktk02.target.com:19888/jobhistory/job/$logDetails "
) | /usr/sbin/sendmail -t
