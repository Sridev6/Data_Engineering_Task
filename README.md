# NewYoker : Data Engineering Exercise

The exercise is designed to use Yelp dataset to complete a series of ETL tasks that is done on python without the usage of any standard frameworks like Spark, SQL Databases. The whole program is packaged on Docker so that it's a plug play thereby avoiding dependency issues and improve processing performance.

**Note** : The whole data to be processed should not be assumed to fit on memory i.e it is required to be processed line by line or in chunks.

## Getting Started

The entire task is designed to consists of 4 modules, 1 abstraction layer and 1 pipeline layer so that the number of modules can be extended or added for maintainability and flexibility purpose.

### Abstraction Layer

The abstraction layer is a custom built Task object that can be used to run any module given the configuration. Following are some of the useful attributes of a task object,

* **Status** - Every task is provided with a state (IDLE, RUNNING, DENIED, SUCCESS, FAILURE) such that at any point in time we know the status of our job. 
* **Configuration** - A configuration is a JSON file that direct the module to be read and certain useful parameters to run the task.

```mermaid
graph LR
A[Square Rect] -- Link text --> B((Circle))
A --> C(Round Rect)
B --> D{Rhombus}
C --> D
