# Peerislands Data Engineering Assignment


## Problem Statement

The task is to create a simple ETL pipeline that extracts data from a text file, transforms the data and loads the data into bronze. silver and gold layers.


## considerations
- the pipeline should be configurable through a configuration file
- pipeline should be written in python or scala 
- the candidate is free to choose any big data or dataframe framework to implement the pipeline(pyspark is preferred)
- the pipeline needs to be deployed to a cloud (gcp preferred).
- having orchestration tools like airflow or luigi is a plus
- logging and observability is a plus


## input data
- input data is a text file with fixed length format path: `cvs/input_datasets/member_dataset.txt`.
- fixed length format specifies the position and length of each field in the file
- fixed length schema is defined in the `cvs/file_layout/file_layout.json` file
- for example if the schema is as follows:
```json
  {
    "fieldName": "memberId",
    "position": 1,
    "length": 10
  }
```
- the first field `memberId` starts at position 1 and has a length of 10 characters in the file



## Data Schema
- `memberId`: member unique identifier.
- `groupId`: member group information .
- `familyId`: member's family id.
- `firstName`: member first name.
- `lastName`: member last name.
- `relationship`: relationship code w.r.t to family id.
- `sexCode`: sex code of the member.
- `dateOfBirth`: date of birth of the member.
- `socialSecurityNumber`: social security number of the member.
- `address1`: address of the member.
- `country`: country of the member.
- `familyType`: family type of the member.
- `phoneNumber`: phone number of the member.
- `effectiveDate`: effective date of the member insurance plan .
- `terminationDate`: termination date of the member insurance plan.
- `plan`: insurance plan code of the member.


## file layout
- the file layout is defined in the `../file_layout/fileLayout.json` file
- file layout specifies the position and length of each field in the file
- file layout helps us in keeping the file extraction logic dynamic, in case of adding new fields or changing the position of the fields in the file, we can update the file layout and the extraction logic will still work.
- to avoid passing down junk or incorrect data to downstream processes, we validate the extracted fields against the validation types specified in file layout. 


## validations
- file layout
```json
        {
          "fieldName": "memberId",
          "position": 1,
          "length": 18,
          "validationType": "required",
          "reactionCode": "MEMBER_ID MISSING",
          "reactionType": "Reject Record"
        }

```

- validation types
    - `required`: field is required
    - `date`: field should be a valid date - assume date format as `YYYY-MM-DD`
    - `optional`: skip validations for this field
    - `list`: field should be in the provided list values
      -  in the example show below we check if the provided value is in the list of `["1", "2", "X"]`
      ```json
      {
        "validationType": "list",
        "validationValues": ["1", "2", "X"]
      }
      ```
   - `checkStringLengthBounds`: 
    -  check if the length of the field is within the provided bounds i.e. min and max
      -  in the example show below we check if the length of the field is between 5 and 12
       ```json
          {
            "validationType": "checkStringLengthBounds",
            "validationValues": {
              "min": 5,
              "max": 12
            }
          }
       ```
  - `regex`: 
    -  check if the field matches the provided regex pattern
      -  in the example show below we check if the field matches the regex pattern `^[0-9]*$`
       ```json
          {
            "validationType": "regex",
            "validationValues": "^[0-9]*$"
          }
       ```
    
## reactions 

- reactions are the actions that we take when a validation fails
- reactions are defined in the file layout
- reactions are of the following types
    - `Reject Record`: reject the record and do not pass this record to the downstream processes
    - `report`: report the error and pass the record to the downstream processes
- for example in the file layout below, if the `memberId` is missing we reject the record
```json
        {
          "fieldName": "memberId",
          "position": 1,
          "length": 18,
          "validationType": "required",
          "reactionCode": "MEMBER_ID MISSING",
          "reactionType": "Reject Record"
        }
```
- if member id is missing for a row, we reject the record create a validation audit stating the reactionCode mentioned


## ETL pipeline

- the ETL pipeline should have the following layers
    - bronze
    - silver
    - gold
    - 
- bronze layer
    - the bronze layer is the raw data layer
    - use the input dataset provided in the `cvs/input_datasets/member_dataset.txt` file
    - make use of the file layout to extract the data from the text file `cvs/file_layout/file_layout.json`
    - the data is extracted from the text file and loaded into the bronze layer
    - the data is validated against the file layout and the validation reactions are applied
    - preferred storage format for bronze layer is parquet with metadata wrappers.
    - (candidate can choose any other format as well as Iceberg, Delta Lake etc.)
- Silver layer 
  - the silver layer is the cleaned data layer, where the records rejected during validation are filtered out
  - the data is transformed and loaded into the silver layer
  - bronze to silver should not have any rejects or corrupted data 
  - make sure the data types of fields are correct
- Gold layer 
  - the gold layer is the aggregated data layer or the final data layer
  - for this assignment it can be one to one mapping between silver and gold layer
  - this layer maybe used for reporting or analytics purposes.
  - preferable storage format is OLTP or NoSql database like Mongodb.




 
