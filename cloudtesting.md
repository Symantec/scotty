# Cloud Health and Cloud Watch writing with scotty

## Background

This document explains how to use command line args and AWS tags to control
how scotty writes data to cloud health and cloud watch.

In order to test writing to cloud health or cloud watch, soctty can run
as either a production instance or a test instance for each system. For
instance, a scotty instance could run in production mode for cloudwatch
but test mode for cloudhealth.

To prevent two scotty instances from writing data for the same machine, the
set of machines for which a test scotty instance writes data and the set of
machines for which a production scotty writes data must be disjoint. To
ensure this, each machine in AWS is designated as "for testing" or 
"for production". For example, a machine in AWS can be designated as 
"for testing" for cloud health but "for production" for cloud watch. We use
AWS tags to make these designations.

## Command Line flags

--cloudHealthTest - If true, the scotty instance is a test instance for
cloud health. The default is false.

--cloudWatchTest - If true, the scotty instance is a test instance for
cloud watch. The default is false.

--cloudWatchFreq - How often to write data to cloudwatch, the default is
"5m" (5 minutes).

## AWS Tags

ScottyCloudWatchTest - If present, the machine is a "test"
machine for cloudwatch which means that only "test" scotty instances
will write data to cloud watch for it.  If not present, it implies the
machine is for "production", and only "production" scotty instances will
write data for it.

ScottyCloucHealthTest - If present, the machine is a "test" machine
for cloudhealth. Only "test" scotty instances will write data for it. If not
present, it implis the machine is for "production" and only "production"
scotty instances will write data for it.

PushMetricsToCloudWatch - If not present, neither "test" nor "production"
scotty instances will write data for this machine to cloud watch. If
present, contains a duration such as "3m" (3 minutes) or "5m" (5 minutes).
which indicates how often data is to be written to cloud health for that
machine. If this tag is present but contains no value, or a value that can't
be converted to a duration, then the --cloudWathFreq flag controls how often
scotty writes data to cloud watch for that machine.


