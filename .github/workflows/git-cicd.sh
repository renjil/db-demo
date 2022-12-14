#!/bin/bash

echo "host: $DATABRICKS_HOST"

# repo_id=$(databricks repos list --path-prefix /Repos/renji.harold@databricks.com/db-demo | jq '.repos | .[].id')
#repo_id=4195692383084558
#echo "Repo ID is $repo_id"

#databricks repos update --repo-id $repo_id --branch staging
#echo "successfully updated staging branch on databricks"

# update databricks job to point to staging branch for executing integration test
