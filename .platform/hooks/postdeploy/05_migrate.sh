#!/bin/bash

# This script runs after the new application version is deployed,
# but before traffic is switched to it. This ensures the database
# schema is updated to match the new code.

echo "--- [postdeploy/05_migrate.sh] Running Database Migrations ---"

# The Flask application's virtual environment needs to be activated
# to get access to the 'flask' command and all dependencies.
source /var/app/venv/*/bin/activate

# Navigate to the root directory of the currently deployed application version.
cd /var/app/current

# Run the Flask database upgrade command.
# The output of this command will be available in the deployment logs
# specifically in /var/log/eb-hooks.log on the EC2 instance.
echo "Executing: flask db upgrade"
flask db upgrade

# Check the exit code of the last command. If it's not 0, something went wrong.
if [ $? -ne 0 ]; then
    echo "!!! [postdeploy/05_migrate.sh] DATABASE MIGRATION FAILED. Deployment will be aborted. !!!"
    # Exit with a non-zero status code to tell Elastic Beanstalk to fail the deployment.
    exit 1
fi

echo "--- [postdeploy/05_migrate.sh] Database Migrations Complete ---"