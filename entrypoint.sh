#!/bin/bash

airflow standalone &

while [ ! -f airflow/standalone_admin_password.txt ]
do
  sleep 2
done

cp --no-preserve=all airflow/standalone_admin_password.txt airflow/secret
tail --follow /dev/null