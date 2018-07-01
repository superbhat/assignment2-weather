# weather

Weather Assignment

Author: Suprabhat Sinha

Run

To compile and test the application:

Create the jar file if using gradle follow below steps
Step 1 - Open terminal/CD prompt
Step 2 - copy the project from GitHub to ur folder
Step 3 - go to cd prompt and go to the directory where you copied the project
Step 4 - run - gradle clean assemble -- this will create a jar file in build folder, go to builder and then to lib and copy that jar
Step 5 - if Hadoop is installed please run below command
/bin/spark-submit --class practice.ambiata_task --master local <jar file name>

Else if gradle is not installed 
Please download the jar file from the project in the GitHub.
And directly run the jar file using below command
/bin/spark-submit --class practice.ambiata_task --master local base-engine-1.0-SNAPSHOT
In this case jar file name is base-engine-1.0-SNAPSHOT.jar

Else 
Copy the code directly to eclipse, then right click on the project, go to the property and select scala compiler and change the scala installation to 2.10 bundle
Scala version is 2.10
And then run the application using run as scala application.


Requirements

There is a weather balloon traversing the globe, periodically taking observations. At each observation, the balloon records the temperature and its current location. When possible, the balloon relays this data back to observation posts on the ground.

A log line returned from the weather balloon looks something like this:

2014-12-31T13:44|10,5|243|AU
More formally this is:

<timestamp>|<location>|<temperature>|<observatory>
Where the timestamp is yyyy-MM-ddThh:mm in UTC.

Where the location is a co-ordinate x,y. And x, and y are natural numbers in observatory specific units.

Where the temperature is an integer representing temperature in observatory specific units.

Where the observatory is a code indicating where the measurements were relayed from.

Data from the balloon is of varying quality, so don't make any assumptions about the quality of the input.

Data from the balloon often comes in large batches, so assume you may need to deal with data that doesn't fit in memory.

Data from the balloon does not necessarily arrive in order.

Unfortunately, units of measurement are dependent on the observatory. The following is a lookup table for determining the correct unit of measure:

Observatory	Temperature	Distance
AU	celsius	km
US	fahrenheit	miles
FR	kelvin	m
All Others	kelvin	km
We need a program (or set of programs) that can perform the following tasks:

a.)Given that it is difficult to obtain real data from the weather balloon we would first like to be able to generate a test file of representative (at least in form) data for use in simulation and testing. This tool should be able to generate at least 500 million lines of data for testing your tool. Remember that the data is not reliable, so consider including invalid and out of order lines.

b.)Produce statistics of the flight. The program should be able to compute any combination of the following on request:

	The minimum temperature.

	The maximum temperature.

	The mean temperature.

	The number of observations from each observatory.

	The total distance travelled.

c.)Produce a normalized output of the data, where given desired units for temperature and distance, an output file is produced containing all observations with the specified output units.

Constraints and Assumptions:-

1. Test file contains data for different observatory and total number of line taken for test is 20 records.
2. Invalid and null value logic removal is shown for Temperature column only same need to be done for other columns as per time constraints not implemented in the project


Formula used:-

MINIMUM TEMPERATURE = MINIMUM FUNCTION USED TO DERIVE THE VALUE
MAXIMUM TEMPERATURE = MAXIMUM FUNCTION USED TO DERIVE THE VALUE
MEAN TEMPERATURE = MEAN FUNCTION USED TO DERIVE THE VALUE
DISTANCE OF (X1,Y1) AND (X2,Y2) = SQUARE ROOT OF((X1-X2)*(X1-X2) + (Y1-Y2)*(Y1-Y2)) IN KM


Data Acquisition Process:-
Data is read from the globe which is revolving around the earth and transmitting to the observatory. Retrieved data from the observatory. 
Also prepared data of observatory specific metrics.
Combined both to get a normalised data.

Data Preparation :-
Created data frames as per the requirement, derived the schema , removed the invalid data and finally normalised output.

Data Cleaning:-
Joined different data frames to get a invalid data to be omitted. In the context invalid or null data is removed by the mean value of the temperature.

Data Modelling :-
Prepared UDF for concatenation, used featured engineering to derived columns needed for calculation.

Solution:-
a.)Given that it is difficult to obtain real data from the weather balloon we would first like to be able to generate a test file of representative (at least in form) data for use in simulation and testing. This tool should be able to generate at least 500 million lines of data for testing your tool. Remember that the data is not reliable, so consider including invalid and out of order lines.
Sol -> as we are going to handle millions of data, will run the code in spark cluster with at least 100 + nodes. Invalid data has been altered with the help of UDF and finally out of oder data from the log is been transformed to schema specific data. Please refer the output for same.

b.)Produce statistics of the flight. The program should be able to compute any combination of the following on request:

	The minimum temperature. 

	The maximum temperature.

	The mean temperature.

	The number of observations from each observatory.

	The total distance travelled.
Sol -> please run the code and perform all the above mentioned operation. Please note if other than these 5 options provided, code will exit.

c.)Produce a normalized output of the data, where given desired units for temperature and distance, an output file is produced containing all observations with the specified output units.
Sol -> output_df is the output data frames which have normalised data as per observatory specific units.

References:

Wikipedia for Latitude and Longitude to calculate the distance between points.
	Formula to covert different measurement of temperatures
	Formula to convert different measurements of distance

