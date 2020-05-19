# Simple Example Dremio Internal Functions

[![Build Status](https://travis-ci.org/dremio-hub/dremio-internal-function-example.svg?branch=master)](https://travis-ci.org/dremio-hub/dremio-internal-function-example)

This shows an example a custom function using Dremio's internal APIs. 

### GetRunidForDate
* Purpose:
   * Demonstrate how to return a Runid for a specific date
   
* Usage example
   ```SELECT GetRunidForDate(CURRENT_DATE) from <VDS>```
     

## To Build and deploy
mvn clean package
cp {target dir} <Dremio Home>/Java/dremio/jars/3rdparty
restart dremio



