# One Hot Encoded Taxi: Analyzing Big Data for New York City Taxis
#CMPT - 732 (Programming in Big Data Project)

New York is one of the busiest cities in the world. People in New York have been using taxis increasingly frequently in the past few years. Currently, in New York City, a few million rides are taken every month. There are many hidden patterns that can be extract to analyze the general behavior of the population as well as the New York Taxi services. We are using a dataset of around 1.5 billion trips to analyze New York City's Yellow Taxi data from 2010 to 2013 to mine various metrics such as driver's yearly salary, surcharge prediction, dispute behavior, passenger count, trips taken on range of miles and many more using Amazon Web Services and PySpark. The size of the raw dataset is around 140 GB. Technologies primarily used includes Python, Spark, Parquet, Tableau, AWS EC2, AWS EMR, AWS S3, AWS IAM for performing analysis.


## Major Analysis
- Surcharge Analysis
> The surcharge analysis includes the programs to find the trips over the term of 4 years which are surcharged ( charged extra ) and it appears that almost 50% of the trips in NYC are surcharged. It also includes a ML (Machine Learning) Algorithm which helps to predict if there is a possibility of surcharge in a trip given the time, and the location of the trip.

- Salary Analysis
> The salary analysis includes programs to find the average monthly/yearly salary of a cab driver in NYC. It appears that that on an average a cab driver in NYC makes around 75k/year. 

- Dispute Analysis
> The dispute analysis includes the programs to find the disputed trips over the period of 4 years. It appears that number of disputes has been increased from 2010 to 2013 because the number of trips is constantly growing. However, the total disputes are decreasing as the credit card payments are increasing over the period.

- Speed Analysis
>The speed analysis includes the programs to divide the trips by the speed in miles/hr to find the number of taxi rides in NYC which are actually completed by breaking the speed limit.

- Tips Analysis
>The tips analysis includes the programs to find the average number of tips received by a cab driver in the span of 4 years. It also includes the trend of tips received by a cab driver in a day/month/year. It appears the cab driver gets paid more tip in afternoon than other time of the day.

- Trips Analysis
> The trips analysis includes the programs to find the number of trips completed by a cab driver in a day/week/month/year. The trips analysis also includes the analysis to check the trend of the trips within a range of miles over the year and month. It appears that the long distance trips (above 50 miles) increases in holidays than the number of short distance trips (less than 10 miles).

- Payment Type Analysis
>The payment type analysis includes the program to find the number of trips completed by using the payment type as cash or credit card. It also includes the amount paid via cash/credit card over the span of 4 years. It appears that the number of cash transactions where high in 2010 whereas the number of credit card transactions are constantly increasing by 2013.


## Interesting Insights from our Analysis

* On average, a cab driver in NYC makes around 75k/year. 

* The number of disputes has been increasing from 2010 to 2013 in response to the increasing number of trips. However, the total disputes are decreasing as the credit card payments are increasing over the same period.

* A cab driver gets more tips in the afternoon than any other time of the day.

* Long distance trips (above 50 miles) increases during the holidays, surpassing the number of short distance trips (less than 10 miles).

* The number of cash transactions were high in 2010, whereas the number of credit card transactions are constantly increasing into 2013.


## Results And Visualization Links

### Project UI is Live at,
http://thedatasets.com/onehotencodedtaxi/

**Results of all our analysis is located in AnalaysisResults folder.**

**All static visualization assoicated with the project are located  in Visalization folder.**
 
### Dynamic Visualization Link
* New York Taxi Trip Analysis
https://public.tableau.com/profile/aakashmoghariya#!/vizhome/NewYorkCitysYellowTaxiTripAnalysis_0/TripAnalysis

* New York Taxi Surcharge Analysis
https://public.tableau.com/profile/aakashmoghariya#!/vizhome/NewYorkCitysYellowTaxiTripAnalysis/PaymentSurcharge

* New York Taxi Tips and Payment Analysis
https://public.tableau.com/profile/ruturaj.patel#!/vizhome/TipsandPaymentAnalysisofYellowTaxi_0/TipsAndPayment
