# Introduction
This repository includes all the relevant code written from the group *Warehouse Wizards* for the DWL-lecture at HSLU. 

To run the code it is best to create a Python virtual environment first and installing the required packages from the file **requirements.txt** to it. Additionally, to run the code files one needs access to different databases (one for the test project, two for the data lake, and one for the data warehouse).  Please use the access details provided in the appendix of our project report to do so.

Below the structure of the repository and what goals the different files serve is described.

# Prototype

The code used for our prototype is structrued along the proccess of the data flow from the source files (different APIs) to the data lake (folder *lambda_data_lake*) and then to the data warehouse (folder *lamda_data_warehouse*; refer to the figure below).

![image](https://github.com/mgrunner/DWL/assets/95408903/9b15dfbe-56e2-4cbf-a1ea-ab14144dc326)

The folder ***lambda_data_lake*** contains the code used in different lambda functions to extract the data from the APIs, transform it and then load it to the data lake. It contains the following files:
* **lambda_free_float_location.py**: Loads data on the available vehicles of free-floating systems (vehicles that doesn't need to be returened to a station) hourly to the data lake. It fetches the data from an API, performs some filtering and matches the vehicles to the neighborhoods in Zurich using geospatial computations before loading it to the data lake.
* **lambda_station_status.py**: Loads data on the available vehicles of station-based systems (vehicles that need to be returned to a station) hourly to the data lake. It fetches the data from an API and performs some filtering before loading it to the data lake.
* **lambda_weatherinfo.py**: Loads data on the information of the current weather in Zurich (e.g. if it is sunny or raining) hourly to the data lake. It fetches the data from an API and selects the relevant fields before loading it to the data lake.
* **lambda_weather.py**: Loads data on the current weather in Zurich hourly to the data lake. It fetches the data from an API and selects the relevant fields before loading it to the data lake.
* **lambda_station_loc.py**: Loads information on the stations of shared mobility services in Zurich to the data lake. It fetches the data from an API, performs some filtering and matches the stations to the neighborhoods in Zurich using geospatial computations before loading it to the data lake.  This code needs to be executed only once.
* **lambda_provider.py**: Loads information on the providers of shared mobility services in Switzerland to the data lake. It fetches the data from an API and performs some filtering before loading it to the data lake. This code needs to be executed only once.
* **lambda_free_float_location.py** (in the folder *old_versions_without_neighborhoods*): Old version of the file *lambda_free_float_location.py* before we matched the locations of the vehicles to a neighborhood. The lambda function is still running but the data is not used for the data warehouse.

The folder ***lambda_data_warehouse*** contains the code used in different lambda functions to populate the data warehouse. It mainly involes a transfer of the data from the data lake to the data warehouse but there is also a source used which is not in the data lake. The folder contains the following files:
* **lambda_data_warehouse.py**: This is the main file populating the data warehouse with the data of the previous day from the data lake. It loads the data from two different databases, combines and transforms them, and then loads them to different tables in the data warehouse. 
* **lambda_static_location_warehouse.py**: Loads information on the different neighborhoods in Zurich to the data lake. The data comes directly from the city of Zurich and is not available in the data lake. This code needs to be executed only once. 
* **lambda_static_provider_warehouse.py**: Loads information on the providers of shared mobility services of relevant vehicle types in Switzerland to the data lake. It fetches the data from the data lake and then loads it to the data warehouse.


# Additional Files
Besides the code beloning to the protype described above there are two additional files available in the repository:
1. File **Exlore_Data.ipynb**: Provides a first insight into the different data sources used for the data lake.
2. Folder **test_project**: Provides the code used in a test project, which was conducted with the aim of testing the feasibility of our project idea. It loaded data from free-floating systems to a databse. As the test project was successfull a lot of the code was reused in the prototype to collect data for free-floating systems.
