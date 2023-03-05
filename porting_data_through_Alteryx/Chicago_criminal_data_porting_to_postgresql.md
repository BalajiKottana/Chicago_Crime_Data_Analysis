In the process of porting data from csv to postgresql instance, I used Alteryx, one of the popular data tool for ETL operations. 

1. Open Alteryx Designer
2. Drag the Input tool into the canvas
3. Connect a file to the input tool, in our case we link the csv file which was downloaded from https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD&bom=true&query=select+*

4. At the end of this step, we will be able to see the sample data of the csv file as shown in the following screenshot. The output of this phase is feed as an input to Select.  
![image](https://user-images.githubusercontent.com/122858293/222927296-4beedbf3-c8f2-44a9-accc-bd3e011f664e.png)

5. In the Select phase, the user can change the name and compatible type of the coulumns and also can select a subset of columns to be ported. At the end of this phase, in the output under "metadata" you can view all the selected attributes and their types as shown below. This output is feed to Data Cleaning phase.
![image](https://user-images.githubusercontent.com/122858293/222927443-ffb533d2-7eab-4a3e-a8e4-39a54605763b.png)

6. In Data Cleaning phase, the user has the provision remove leading and trailing whitespaces, tabs, newlines and duplicate whitespaces in the data. The output of this phase is feed to output.  
![image](https://user-images.githubusercontent.com/122858293/222927695-0d13612e-0ba8-48e0-9ade-0749d7133c52.png)

7. In output phase, by clicking "Set Up Connection" choose postgresql ODBC connection to connect the db local instance. At this step the data is ported to the database. 
![image](https://user-images.githubusercontent.com/122858293/222927772-797214de-3e76-4824-9f93-56af31c81c10.png)

These are basic steps used to port data and this orchastrated process can be saved and used for later usage. 

In similar fashion, I ported police_station master and iucr_code master tables.
