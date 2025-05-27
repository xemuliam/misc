## Script materialisation

This materializations allows to execute free-form SQL script as DBT model. Thus now you are not limited by just single SELECT statement inside the model 😉

It could be really helpful if you need to perform e.g. stored procedure call, or atomic insert, or atomic update. 
Or if you want to run really complex SQL script with logic expressed by many statements and variables. 
Or if you need to deal with late arriving data. 

**Additional benefit:**
by using script materialization you can eliminate major DBT limitation:

**_"each target DB table must be addressed by one and only DBT model"_**

That means you can have table model my_model and as many script models which are changing something in that model depending on your business requirements.

<ins>Example:</ins>
```sql
{{ config(
    materialized='script',
)}}

declare test_val default 123;

insert into {{ source('sandbox', 'dml_first') }}
values (
  'test script', 1, 1.0, current_timestamp
);

update {{ ref('dml_first') }}
set field2f = 1.01
where true
  -- and 1/0
  and field0s = 'test script'
```

### Bulk mode

Also, I've added capability to run that script model in bulk or script mode. Some of data managem,ent systems don't allow scripting and run each individual statement from the script as separate independent query. Thus you have boolean config parameter `bulk` to control this behaviour.
Default value of this pamater is `false`, which runs SQL from model as script and allows to mimic current bahaviour for call statements or run_query macro.
However, if you need to switch this materialisation to bulk mode (run each statetemnt from inside as separete quey against your data management system), then just use this:

```sql
{{ config(
    materialized='script',
    bulk=True,
)}}
```


### !!! Important !!!
Whereas script could affect many tables inside, this materialization returns empty target relation. Thus if you need to have dependency on script model then you must [force denedency](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies):
```sql
{{ config(
    materialized='script',
)}}

-- depends_on: {{ ref('script_0') }}

truncate table {{ ref('dml_first') }};

insert into {{ source('sandbox', 'dml_first') }}
values (
  'test script 999', 999, 999.0, current_timestamp
)
```

---


## Copy materialisation

BigQuery allows to easily [copy table data and metadata](https://cloud.google.com/bigquery/docs/managing-tables#copy-table) into different table.

Copy operations will be performed as BigQuery COPY jobs using BigQuery storage API. It won't be like `create or replace table as select`. SQL engine won't be in use while data and metadata will be copied from one table to another. Moreover, COPY jobs are free of charge.

I tested that by copying table with 2TB data inside. It was copied within 45 seconds in my environment. And again: it costed me nothing! Impressive, huh? 😜


This materialisation implements BigQuery copy API and lets you perform:
* append or rewrite target table using:
    * copy single source table
    * copy multiple source tables
* rewrite target partitons (not the whole table) by several individual partitions exist in single source table


This materialization also allows to make union-all-like operation even in case different (but compatible with target) structures w/o any additional efforts from developer side.
(see how it works on following screenshots).

And it is able to rewrite nof only full target table but also only individual partitions. Imagine you have 100 partitions in target and only 3 on source table. Now you can copy those 3 partitions replacing corresponding target partitions like truncate partition and insert but w/o SQL, just using copy API. Other 97 target partitions remain intact.

### Materialization parametes list with acceptable values:
- **copy_materialization:** rewrite or append data to target table. Possible values are `table` and `incremental`. Default value if not defined is `table`
- **interim_database:** desired BigQuery project to store interim table (to process multiple sources with different structure). Model project will be in use if not defined.
- **interim_schema:** desired BigQuery dataset to store interim table (to process multiple sources with different structure). If not defined then model dataset will be in use.
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
    materialized='copy',
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
![image](https://github.com/xemuliam/misc/assets/20856221/e2d88a9b-0728-4a9b-9eb8-07fe24f94efb)
