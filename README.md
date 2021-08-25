# Introduction 
Utility functions and classes for working with Dataframes, provisioning SparkSession and much more.

# Getting Started
Spark Utils is part of Spark AKS image. If you want a new version to be used by actual runtime, make a PR for our [Spark image](https://dev.azure.com/eccoSneaksAndData/commonInfrastructure/_git/spark)

# Build and Test
Test pipeline runs Spark in local mode, so everything can be tested against our current runtime. Update the image used in `build.yaml` if you require a test against a different runtime version.