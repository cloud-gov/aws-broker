## aws-rds

Quick and dirty Go program used in database broker smoke tests.

### Usage

1. `cf create-service <service> <oracle-plan-name> <oracle-instance-name>
1. `cf create-service <service> <posgres-plan-name> <posgres-instance-name>
1. wait for service creation to finish
1. `cf push --var oracle-service=<oracle-instance-name> --var pg-service=<postgres-instance-name>`
1. If the app starts successfully, your brokered database service was able to be written to.

### Notes

This tool vendors some Oracle binaries, which are licensed separately. You can find them under the `include/oracle` library, and the license at `include/oracle/BASIC_LICENSE`.

This repo requires `git lfs` - be sure to `git lfs install` before trying to push any apps
