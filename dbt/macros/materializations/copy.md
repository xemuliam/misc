### New version of copy materialisation.

This new copy materialization allows to make union-all-like operation even in case different (but compatible with target) structures w/o any additional efforts from developer side.

#### Features:
- implements BigQuery copy API
- implements "fail fast" approach. maximum number of checks will be performed on DBT side, w/o touching database
- checks if there is at least one relation in model and raise compilation error if not
- checks whether all realtions are tables and raise compilation error if not
- checks whether model relations have table structure compatible to target table and raise compilation error if not
- checks wheter fully qualified relations are unique across all model relations (BQ copy API disallows usage more than one the same fully-qualified table as source) and raise compliation error if not
- if all model relations have the same structure then single copy job will be initiated using full list of relations as source
- if relations structures are different then to group model relations with similar structure to run single copy job per each group to minimize number of copy jobs. For example if we have 20 relations in model, 15 of them have similar structure, and rest 5 have also similar (bur different from those 15) structure then only 3 copy jobs will be performed (see below why 3 but not 2)
- if we have more than one relations group then follwing steps will be performed:
  - interim table will be created in models project and dataset (to put into different project and/or dataset please use `interim_database` and/or `interim_schema` config parameters). if `copy_materialization parameter` is `incremental` and target table exists then its structure will be in use to create interim table, otherwise table structure will be got from first relation in model
  - series of copy jobs will be run sequentially to copy information from model relations groups into interim table
  - copy job from interim table to target
  - drop interim table
- interim table is required to eliminate situation when we have sevel operations against target but not all of them will be successful. we perform several operations against interim table and then- single operation against target
- interim table has unique name for each execution. this eliminates the isuue with fast reaching `table change per day` limit
- able to rewrite individual target partitions instead of full table


#### Materialization parametes list with acceptable values:
- **copy_materialization:** rewrite or append data to target table. Possible values are `table` and `incremental`. Default value if not defined is `table`
- **interim_database:** desired BigQuery project to store interim table, Model project will be in use if not defined. If not defined then model project will be in use.
- **interim_schema:** desired BigQuery dataset to store imterim table, If not defined then model dataset will be in use.
- **copy_partitions:** rewrite only those target partitions which exist in source table. Possible values are `true` and `false`. If **copy_materialization** is `incremental` then this parameter will be ignored

=========
 
Here is examples of model relations structures:

![image](https://github.com/xemuliam/misc/assets/20856221/ec9f66ef-6385-4a00-b593-e7dc33eae27e)

![image](https://github.com/xemuliam/misc/assets/20856221/501d54b3-1ca8-4ce3-8779-a57485b35dea)

![image](https://github.com/xemuliam/misc/assets/20856221/04d9a5fe-e2c1-455c-941a-9fa195b9a27d)

![image](https://github.com/xemuliam/misc/assets/20856221/15cd0314-9358-43dc-8815-3aa79d9698f4)

![image](https://github.com/xemuliam/misc/assets/20856221/a96d8197-4258-4ecd-a354-b74b5180f623)

![image](https://github.com/xemuliam/misc/assets/20856221/a6ac9a69-f8f2-44bf-822a-799c010daccc)

Here is model example:
```sql
{{ config(
    materialized='copy_new',
    copy_materialization='incremental',
    interim_database='internal',
    interim_schema='test_dataset',
)}}

{{ source('sandbox', 'table_a') }}
{{ ref('table_a_cp') }}
{{ ref('table_c') }}
{{ ref('table_b') }}
```

And here is the result:
![image](https://github.com/xemuliam/misc/assets/20856221/d067d3e6-b724-447d-9cf1-499b3ead22e8)

Here you can see compatibility check for above tables:
![image](https://github.com/xemuliam/misc/assets/20856221/013074b3-86ee-45f0-9b14-f285af8559e9)
