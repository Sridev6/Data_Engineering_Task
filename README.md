﻿# NewYoker : Data Engineering Exercise

The exercise is designed to use Yelp dataset to complete a series of ETL tasks that is done on python without the usage of any standard frameworks like Spark, SQL Databases. The whole program is packaged on Docker so that it's a plug play thereby avoiding dependency issues and improve processing performance.

**Note** : The whole data to be processed should not be assumed to fit on memory i.e it is required to be processed line by line or in chunks.


# Table of Contents  
* [Notable Features](#notable-features)
	* [Overview](#overview)
	* [Abstraction Layer](#abstraction_layer)
		* [Executor](#executor)
		* [Task](#task)
	* [Modules](#modules)
	* [Pipeline Layer - DAG](#pipeline-layer)
		* [Simple Pipeline](#simple-pipeline)
		* [Complex Pipeline](#complex-pipeline)
* [Getting Started](#getting-started)
	* [Prerequisites](#prerequisites)
	* [Installing](#installing)
		* [Execution with Task objects](#execution-with-task-objects)
		* [Execution with Dag object](#execution-with-dag-object)
* [Performance Test](#performance-test)
* [Author](#author)

<a name="notable-features"></a>
## Notable Features

The entire task is designed to consists of 4 modules, 1 abstraction layer and 1 pipeline layer (DAG) so that the number of modules can be extended or added for maintainability and flexibility purpose.

<a name="overview"></a>
### Overview 

The Abstraction layer has the ability to run every module independent from each other. Also, a single module can be executed in parallel with different configurations on different docker containers to improve performance.


<img align="centre" width="500" height="500" src="https://github.com/Sridev6/Data_Engineering_Task/blob/master/diagrams/task_abstraction.jpg">

<a name="abstraction_layer"></a>
### Abstraction Layer

The Abstraction Layer consists of 2 objects :

<a name="executor"></a>
#### Executor

The Executor is an abstraction that connects to a configuration file. It's a custom built object that creates a task object (see next section) based on the configuration file (see task abstraction diagram). A task configuration file is assigned to the executor based on the arguments that is run from docker console. 

<a name="task"></a>
#### Task

The core of the abstraction layer is a custom built Task object that can be used to run any module given the configuration. Following are some of the useful attributes of a task object,

* **Status** - Every task is provided with a state (IDLE, RUNNING, DENIED, SUCCESS, FAILURE) such that at any point in time we know the status of our job. 
* **Configuration** - A configuration is a JSON file that direct the module to be read and certain useful parameters to run the task.

<a name="modules"></a>
### Modules

As explained above, there are 4 modules associated with this exercise. Every module is called by the Task object which is defined based on the task configuration file. Following are the modules and it's functions :

* **Extract and Clean** - A module to parse, clean and extract data from the tar file. Also, config parameters allow to extract specific files / JSON column data rather than all the files and JSON column from the tar file. All records in the NEWLINE delimited JSON is parsed line by line to reduce memory failures since its processing huge files ( up-to 5 GB). The extracted, cleaned and clipped data file is written again to a NEWLINE delimited JSON file.

* **Sample Users** - A module to clip X users from the extracted/cleaned user JSON file and save the results to a CSV file with headers. The X can be defined inside the configuration file for the task. The JSON file is parsed in chunk lines to avoid multiple IO operations. Note : If required, the chunk can be set to 1 to parse line by line.

* **Sample Users' Reviews** - A module to select all reviews for the sampled users and write the results to a CSV file with headers. The parsing procedure is the same as sample users module.

* **Sample Users with no review in last year** - A module to select all user IDs from the sampled set who did not write any reviews from the specified X date. The X date can be given in the configuration file which can be changed any time. The parsing procedure is the same as sample users module.

<a name="pipeline-layer"></a>
### Pipeline Layer - DAG

Though task object can run every module independent from each other, an ETL process will sometimes have dependency tasks before it can begin, hence the need for a pipeline.  The pipeline layer is a custom built dag object that orchestrates the task objects in parallel or sequential given the DAG configuration file. The DAG configuration is a series of tasks on a JSON file which has its dependencies defined in it. An example configuration JSON looks like this,


```json
{  
  "decompress_and_clean" : {  
    "path" : "{base}/configs/decompress_and_clean.json"  
  },  
  "sample_users" : {  
    "path" : "{base}/configs/sample_users.json",  
  "dependencies": [  
      "decompress_and_clean"  
  ]  
  }
}
```   

<a name="simple-pipeline"></a>
#### Simple Pipeline :
A simple pipeline is a series of tasks that runs in sequence. One task is dependent on another to begin execution. Following is how a simple pipeline works,

![Diagram](https://github.com/Sridev6/Data_Engineering_Task/blob/master/diagrams/dag_simple_pipeline.jpg)

<a name="complex-pipeline"></a>
#### Complex Pipeline :
It's good to have task dependancy. But there are some tasks completely independent from each other. A complex pipeline serves this purpose i.e. to run independent tasks in parallel and wait until all the parallel task has completed before moving to the next step in the pipeline. Following is how a complex pipeline can be work,

![Diagram](https://github.com/Sridev6/Data_Engineering_Task/blob/master/diagrams/dag_complex_pipeline.jpg)

**Note** : An advanced version can be done using Airflow. This is just a complete custom built Pythonic version to showcase object oriented programming in action.

### Notify Failures by Email
When the pipeline features is scheduled to run on a scheduler, then when something goes wrong we need to be notified. Hence, any task failures is sent as an email to set recipients.  The email addresses can be updated here,
 ```
Data_Engineering_Task/container_folder/newyoker_task/etl/__init__.py
  ```

A sample failure notification will look like this,

![Diagram](https://github.com/Sridev6/Data_Engineering_Task/blob/master/diagrams/task_error.png)


<a name="getting-started"></a>
## Getting Started

To run the project we need to clone this repository into our local machine and follow the instructions given below.

<a name="prerequisites"></a>
### Prerequisites
All we need is to have **docker** installed in the machine. Please find the download files [here](https://www.docker.com/get-started) and the installation documentation [here](https://docs.docker.com/install/).

Also, the yelp dataset is assumed to downloaded into the local machine in any directory. The dataset can be found [here](https://www.yelp.com/dataset/download)

<a name="installing"></a>
### Installing

From the notable features section, we understand that the project can be run with independent tasks multiple times or run a dag pipeline once, that triggers all the tasks based on the dependencies.

Here we will see how to run the project using independent tasks multiple times and also using dag feature once.

**Step 0** : Create a folder in the local machine. For simplicity let's call this folder as "data/" from henceforth. Move the tar file to this folder. This folder will be mounted on to the docker and will be used for reading and storing results of every module.

Initial contents of the folder,

 ```
 {ANY_DIRECTORY}/data/yelp_dataset.tar
  ```

<a name="execution-with-task-objects"></a>
#### Execution with Task objects :

We have 4 modules which can be executed individually. Following are the modules which we will run,

1. Extract and clean (Process data)
2.  Sample Users (Process data)
3. Get all reviews of sample users (Query data)
4. Get all user IDs who didn't write any review in the last year (Query data)

Every module is dependent one other module except the decompress and clean module (Module 1). So we can run Module 1 with different arguments in different containers to make the extract process quicker.

##### Module IO Dependencies 
* **Module 1** 
	* *Input* : data/yelp_dataset.tar file
	* *Output* : data/cleaned_review.json (or) data/cleaned_user.json 
* **Module 2**
	* *Input* : data/cleaned_user.json
	* *Output* : data/sample_users.csv 
* **Module 3**
	* *Input* : data/sample_users.csv
	* *Output* : data/sample_users_review.csv 
* **Module 4**
	* *Input* : data/sample_users_review.csv
	* *Output* : data/sample_users_no_review_in_last_year/0.csv,  data/sample_users_no_review_in_last_year/0.csv.

**Note** : Module 4's output is stored inside a folder as it will have multiple output files like '0.csv', '1.csv' and so on. The filename with the largest number will contain the results of the module. All other files are generated by the recursion process as meta data.


A task configuration is pre-defined for every module and stored inside,
  ```
Data_Engineering_Task/container_folder/newyoker_task/main/task/configs 
 ```
 Following are the steps to follow to run any task for modules,
 
 1. Open terminal, change to local path to the git repo until
```
  {ANY_DIRECTORY}/Data_Engineering_Task/
```
   2. Build Dockerfile
```
  docker build -t {any_build_name} .
```
  3. Run docker container
```
  docker run -v /{LOCAL_DIRECTORY_TO_DATA_FOLDER}/data:/container_folder/newyoker_task/data {any_build_name} python -u main/task/execute.py {task}
```

So, in order to run a module all we need to do is follow the above three steps and update "{task}" with the following python arguments,

* **review** - to extract, clean and clip specific fields from review.json in tar file.
* **user** - to extract, clean and clip specific fields from user.json in tar file.
* **sample_users** - to sample around 1% of the users from the cleaned_user.json file.
* **sample_users_review** - to get all reviews of sample users.
* **sample_users_no_review_within_time_interval** - to get all user IDs who didn't write any review in the specified time interval. The time interval is specified inside the task configuration file.

**Note** : Since there are IO dependencies for every module, it's required to run these modules in sequence. If not, an IO error will be thrown. 

<a name="execution-with-dag-object"></a>
#### Execution with Dag object :

Similar to predefined task configurations, task we have predefined dag configurations stored inside the following directory,

```
Data_Engineering_Task/container_folder/newyoker_task/main/dag/configs 
 ```

Following are the steps to follow to run any dag,

1. Open terminal, change to local path to the git repo until
```
  {ANY_DIRECTORY}/Data_Engineering_Task/
```
   2. Build Dockerfile
```
  docker build -t {any_build_name} .
```
  3. Run docker container
```
  docker run -v /{LOCAL_DIRECTORY_TO_DATA_FOLDER}/data:/container_folder/newyoker_task/data {any_build_name} python -u main/dag/execute.py {dag}
  ```
  
So, in order to run a dag all we need to do is follow the above three steps and update "{dag}" with the following python arguments,

* **complete_pipeline** - This dag will run all the 4 modules as follows,
![Diagram](https://github.com/Sridev6/Data_Engineering_Task/blob/master/diagrams/dag_complete_pipeline.jpg)

* **query_pipeline** - This dag will only run Module 2, 3 and 4 with Module 3 and 4 in parallel. Assuming Module 1 have been already run. (Because Module 1 is a one time task. Once done, it's not required to decompress the data again.)

![Diagram](https://github.com/Sridev6/Data_Engineering_Task/blob/master/diagrams/dag_query_pipeline.jpg)

<a name="performance-test"></a>
## Performance Test

Every task/module was tested on docker container with 4 cores and 8 GB RAM. Following are the performance test results for every task module,

* **Extract and Clean Process**
	* review.json file : < 200 seconds.
	* user.json file : < 120 seconds.
	
**Note** : When the above 2 tasks run in parallel on different docker containers, both tasks tend to complete <250 seconds.

* **Query Process**
	* Sample Users : < 1 second.
	* Sample Users Review : < 40 seconds.
	* Sample Users with no review in last year : < 40 seconds.



<a name="author"></a>
## Author
* [Sridev Srikanth](https://linkedin.com/in/sridevsrikanth/)

## Improvements 
* Multiple nested objected can be added to the Task object to contain state of meta data and and re-take the task for the next time when it's executed.

* A scheduler can be implemented to run a dag pipeline so that there is no need for any external pipeline to schedule this task. The project can run independently on its container.
