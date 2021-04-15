#!/bin/bash

mvn package
hadoop jar target/CHI_SQUARE_CALCULATION_JOB-1.0-SNAPSHOT.jar /user/pknees/amazon-reviews/full/reviewscombined.json  output
