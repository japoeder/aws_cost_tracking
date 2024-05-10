# Slack Alerting for AWS

It's really easy for AWS costs to slip under the radar and accrue to unexpected amounts.  While Amazon does provide some functionality with their cost explorer meant to address that, it is not as flexible in how the data can be aggregated and it isn't integrated with Slack (a commonly used IM platform) as far as I'm aware.

## Objectives

- Alert users if there is a reversal in the trend of a given AWS service's cost the day prior
- Alert users if there is a spike in a given AWS service's cost the day prior
- Provide month to date totals by service
- Provide totals across services by day

### Running cost reporting

1. Download and install Python on your computer
2. Clone this repo, and navigate to aws_cost_tracking directory (containing the README.md)
3. Set up environment by running the following in the terminal:

   1. conda create --name aws_cost_tracking python=3.10
   2. conda activate aws_cost_tracking
   3. conda config --add channels conda-forge
   4. conda install --file requirements.txt

## Project Structure

```
.
├── _images/                          : Samples of output visualization 
├── _util/                            : Utilities to gather credentials, loop through viz creation, etc.
├── procAmzAlerts.py                  : Process alerts for AWS services where appropriate
├── procAmzData.py		      : Extract Cost Explorer data via API and process
├── procPlotting.py		      : Driver for generating corresponding plotting
├── procSummaries		      : Program to create summary data for Slack payload
├── runCostReporting.py		      : Primary driver program for alerting framework
└── README.md                         : Project overview 
```

## Methodology

#### Cost spikes

To determine if a spike has occured in a given day we look at daily data over the previous year, calculate the average daily spend by service as well as the standard deviation.  This is then used to calculate the z-score as follows:

<p align="center">
  <img width="120" height="50" src="https://github.com/japoeder/collectability_cost_tracking/blob/master/_images/zscore.jpeg?raw=true">
</p>

where *x* is the daily cost value, mu is the mean, and sigma is the standard deviation.  Once the scores are calculated across days and services, if the z-score is above a predefined threshold, and alert is sent via the Slack API for that service.  Here is a sample image of an alert:

<p align="center">
  <img width="650" height="400" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/spike_alert_example.png?raw=true">
</p>

Note the prior day is highlighted in the image, but the code has been updated to highlight every day that is above the predefined threshold.  The current setting is one standard deviation above the mean, though this can be *increased* to reduce the number of alerts that will be thrown.

#### Cost trend reversals

In a similar fasion, we look at daily spend over the last year but instead calculate the MACD to determine if the cost trend has reversed direction.  The **Moving Average Convergence/Divergence** is a momentum oscillator primarily used to trade trends. It appears on the chart as two lines which oscillate without boundaries.

<p align="center">
  <img width="650" height="400" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/trend_alert_example.png?raw=true">
</p>

When the MACD crosses down the 18EWM, the trend reverses downward and vice versa when it crosses up (as seen in the image).  MACD is calculated as:

<p align="center">
  <img width="275" height="50" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/macd_line.jpeg?raw=true">
</p>

and the 18 day EMA as:

<p align="center">
  <img width="275" height="50" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/signal_line.jpeg?raw=true">
</p>

#### Summary plots

Example charts of the summary data provided are as follows:

<p align="center">
  <img width="650" height="400" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/mtd_x_svc_example.png?raw=true">
</p>

<p align="center">
  <img width="650" height="400" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/svc_x_day_example.png?raw=true">
</p>

#### Alerting

These flow through to the user in the form of Slack messages, and are formatted as follows:

<p align="center">
  <img width="650" height="400" src="https://github.com/japoeder/aws_cost_tracking/blob/master/_images/slack_bot_example.png?raw=true">
</p>

When clicking on the 'Plot' button, it opens the appropriate plot as shown in the examples above.  The Slack image only shows the summary ones, but there are sections for upward trending costs, downward trending costs, and spikes that cross the predefined z-score threshold.

## Architecture

Standing this up requires:

* AWS RDS MySQL database to store data (others can be used, but appropriate modifications to the code are required)
* AWS S3 bucket to store the plots
  * Note that this also needs to configured such that it can serve a static page
  * IP filtering is recommended to prevent unauthorized users from accessing sensitive data
* Slack bot with appropriate profiles / permissions
  * You'll need to create a token for authentication
  * The channel ID where alerts should be sent is also needed

For automation (recommended):

* AWS EC2 instance (lowspec) running Linux to kick off the repo with crontab
