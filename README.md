# Run postgres in Kubernetes
1. cd /Users/eliot/projects/HEIR/code/postgres/yaml
2. If there is no postgresql namespace already created, then:
   kubectl create ns postgresql
3. kubectl apply -f pv.yaml
4. kubectl apply -f pvc.yaml
5. helm install postgresql -f values.yaml bitnami/postgresql -n postgresql --set volumePermissions.enabled=true
6. kubectl port-forward --namespace postgresql svc/postgresql-dev 5432:5432 &
    PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U eliot -d synthea -p 5432
7. load database with: psql -h localhost -U eliot -d synthea -p 5432 -f /Users/eliot/projects/HEIR/synthea/syntheaExport.sql
   (Note: if this give an error that the synthea db is not found, use the testK8s data to connect to the db and then create synthea - e.g.:
   psql -h localhost -d testK8s
    create database synthea)
8. Repeat steps 2-7 using the postgresql1 namespace and a different image name and host port number
   1. kubectl create ns postgres1
   2. kubectl apply -f pv1.yaml
   3. kubectl apply -f pvc1.yaml
   4. helm install postgresql1 -f values1.yaml bitnami/postgresql -n postgresql1 --set volumePermissions.enabled=true
   5. kubectl port-forward --namespace postgresql1 svc/postgresql1 5434:5432
   6. psql -h localhost -U eliot -d synthea1 -p 5434 -f /Users/eliot/projects/HEIR/synthea/syntheaExport.sql
9. Install Fybrik from the instructions in the Quick Start (https://fybrik.io/dev/get-started/quickstart/)
   (Note: if this give an error that the synthea db is not found, use the testK8s data to connect to the db and then create synthea - e.g.:
   psql -h localhost -d testK8s
    create database synthea;
    exit)
## STEPS 1-3 have already been done
Populate a FHIR database in postgresql.  Note that only the dockerized Fhirbase installation works - the non-dockerized version 
uses an old version of golang.  Fhirbase is used to create the SQL tables for FHIR resources and to load a bundle of FHIR data 
into the appropriate tables.  Instead, do all of this work in the docker version and export the loaded database via a bind mount 
to the host.
1. docker run --rm -p 3000:3000 -v /Users/eliot/temp:/mnt -d fhirbase/fhirbase:latest
2. docker exec -it de101168efff /bin/bash    [Substitute the container id from fhirbase here...]
	From inside the docker container:
	psql
	  > create database synthea1;
	  > \q      [exit postgres]
       fhirbase -d synthea1 --fhir=3.0.1 init     # Creates database with name synthea1    
       for file in /mnt/female/*json;
        fhirbase -d synthea1 --fhir=3.0.1 load $file;
       pg_dump synthea1 > /mnt/syntheaExport.sql          # export the database
       exit     # leave the docker container
3. pg_load test1 < /User/eliot/temp/exportDB.sql

With Fybrik running:
4. Load the policy:
   misc/applyPolicy.sh
5. Load the asset:
   kubectl apply -f yaml/asset.yaml
6. kubectl port-forward svc/opa 8181 -n fybrik-system

## To run Presto
Catalogs describing databases in /Users/eliot/projects/HEIR/code/postgres/presto-server-0.279/etc/catalog
Make sure the correct external IP for k8s postgres server is configured in the postgresqlk8s.properties and postgresl.properties 
file!
Check if port 8080 is taken!:  On the Mac: lsof -i :8080
> presto-server run
NOTE: stop and restart server if the catalog is configured!!

Test the Presto connection:
from /Users/eliot/projects/HEIR/code/postgres/presto-server-0.279
./presto --server localhost:8080 --catalog postgresql --catalog postgresql1 --schema public

Query:
select * from postgresql1.public.patient LIMIT 5;

## To test:
misc/curlPatient.sh
misc/curlResearcher.sh

# Developer mode
### Kubernetes directions for loading database on postgres - only do this once
1. kubectl edit svc/<svc name> and change type to "LoadBalancer"
2. load database with: psql -h localhost -U eliot -d synthea -p 5432 -f /Users/eliot/projects/HEIR/synthea/syntheaExport.sql
## Try some queries
psql -h localhost -p 5432 -d synthea1
select resource from patient limit 5;
select resource->'name' from patient limit 5;
  [{"use": "official", "given": ["Abram53"], "family": "Hickle134", "prefix": ["Mr."]}]
 [{"use": "official", "given": ["Aaron697"], "family": "Bins636", "prefix": ["Mr."]}]
 [{"use": "official", "given": ["Aaron697"], "family": "Blick895", "prefix": ["Mr."]}]
 [{"use": "official", "given": ["Abram53"], "family": "Hoeger474", "prefix": ["Mr."]}]
 [{"use": "official", "given": ["Abram53"], "family": "Nicolas769"}]
(5 rows)
select resource->'name'->0->'family' from patient limit 5;
   ?column?   
--------------
 "Hickle134"
 "Bins636"
 "Blick895"
 "Hoeger474"
 "Nicolas769"
(5 rows)

## Testing SQL commands
from /Users/eliot/projects/HEIR/code/postgres/presto-server-0.279
./presto --server localhost:8080 --catalog postgresql --catalog postgresql1 --schema public

Query:
select * from postgresql1.public.patient LIMIT 5;
select * from postgresql.public.patient LIMIT 5;

# SEE https://docs.aws.amazon.com/athena/latest/ug/extracting-data-from-JSON.html

=====
 with s as (with source1 as (select id ID, resource RESOURCE, json_extract(resource, '$.subject') subjects from 
postgresql.public.observation) select json_extract_scalar(subjects,'$.id') PATIENT_ID, json_extract(resource, '$.value') VALUE, 
json_extract(resource,'$.code') CODE from source1) select * from s where patient_ID = 'urn:uuid:f9e2ec3e-731f-4341-8a6f-dffa5a44b373';




WITH 
subjects as (select json_extract(resource, '$.subject') FROM postgresqlk8s.public.observation subjects), 
attributes as (select * FROM postgresqlk8s.public.observation attributes), 
subjects1 as (select json_extract(resource, '$.subject') FROM postgresql.public.observation subjects), 
attributes1 as (select * FROM postgresqlk8s.public.observation attributes1) 
SELECT json_extract_scalar(subject,'$.id') ID, attributes.resource ATTRIBUTES 
FROM subjects as t(subject), attributes 
WHERE json_extract_scalar(subject,'$.id')='08723d97-8dd3-4481-a5f1-a9427488d729' 
UNION ALL SELECT 
json_extract_scalar(subject1,'$.id') ID, attributes1.resource ATTRIBUTES 
FROM subjects1 as t(subject1), attributes1 
WHERE json_extract_scalar(subject1,'$.id')='urn:uuid:ad023201-471e-4780-b424-0eb172c074f2' limit 2;
                      ID                       |                                                                                                                                
>
-----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------->
 urn:uuid:ad023201-471e-4780-b424-0eb172c074f2 | 
{"category":[{"coding":[{"code":"vital-signs","display":"vital-signs","system":"http://terminology.hl7.org/CodeSystem/observati>
 urn:uuid:ad023201-471e-4780-b424-0eb172c074f2 | 
{"category":[{"coding":[{"code":"vital-signs","display":"vital-signs","system":"http://terminology.hl7.org/CodeSystem/observati>



WITH 
subjects as (select json_extract(resource, '$.subject') from postgresqlk8s.public.observation subjects), 
attributes as (select * from postgresqlk8s.public.observation attributes), 
subjects1 as (select json_extract(resource, '$.subject') from postgresql.public.observation subjects), 
attributes1 as (select * from postgresqlk8s.public.observation attributes1)
SELECT json_extract_scalar(subject,'$.id') ID, attributes.resource ATTRIBUTES 
FROM subjects as t(subject), attributes 
WHERE json_extract_scalar(subject,'$.id')='08723d97-8dd3-4481-a5f1-a9427488d729' 
UNION ALL 
SELECT json_extract_scalar(subject1,'$.id') ID, attributes1.resource ATTRIBUTES FROM subjects1 as t(subject1), 
attributes1 WHERE json_extract_scalar(subject1,'$.id')='urn:uuid:ad023201-471e-4780-b424-0eb172c074f2';

=====
with subjects as (select json_extract(resource, '$.subject') from postgresqlk8s.public.observation subjects), 
attributes1 as (select * from postgresqlk8s.public.observation attributes) select cast(json_extract_scalar(subject,'$.id') as varchar) ID, attributes.resource ATTRIBUTES from subjects as 
t(subject), attributes where json_extract_scalar(subject,'$.id')='urn:uuid:ad023201-471e-4780-b424-0eb172c074f2' limit 2;

with subjects as (select json_extract(resource, '$.subject') subjects from postgresqlk8s.public.observation) select cast(json_extract_scalar(subject,'$.id') 
as varchar) ID from subjects as t(subject) where json_extract_scalar(subject,'$.id')='urn:uuid:ad023201-471e-4780-b424-0eb172c074f2' limit 2;
=====

select * from table_name where false

presto:public> select * from organization where false;
 id | txid | ts | resource_type | status | resource 
----+------+----+---------------+--------+----------
(0 rows)

with subjects as (select json_extract(resource, '$.subject') subjects from postgresqlk8s.public.observation) select json_extract_scalar(subject,'$.id') id 
from subjects as t(subject) limit 2;
                      id                       
-----------------------------------------------
 urn:uuid:93435295-0ca3-4846-abb6-8faba6fcd3f6 
 urn:uuid:93435295-0ca3-4846-abb6-8faba6fcd3f6 


select json_extract(resource, '$.category') SUBJECT from postgresqlk8s.public.observation limit 2;
-------------------------------------------------------------------------------------------------------------------------------------
 [{"coding":[{"code":"survey","display":"survey","system":"http://terminology.hl7.org/CodeSystem/observation-category"}]}]           
 [{"coding":[{"code":"vital-signs","display":"vital-signs","system":"http://terminology.hl7.org/CodeSystem/observation-category"}]}] 
(2 rows)

presto:public> select json_extract(resource, '$.subject') SUBJECT from postgresqlk8s.public.observation limit 2;
                        SUBJECT                         
--------------------------------------------------------
 {"id":"urn:uuid:31665402-bfc5-45e1-b12a-fdc42836f50a"} 
 {"id":"urn:uuid:31665402-bfc5-45e1-b12a-fdc42836f50a"} 
(2 rows)

select json_extract_scalar(resource, '$.id') ID from postgresqlk8s.public.patient limit 2;
RETURNS:
                  ID                  
--------------------------------------
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 
 096724e0-4f46-4af9-8d25-2e5d6ec47526 
(2 rows)


select json_extract_scalar(fname, '$.family') fname from patient cross join unnest(cast(json_extract(resource, '$.name') as array(json))) as y(fname) limit 2;

      fname      
-----------------
 Hickle134       
 Bins636 

select id ID, json_extract_scalar(fname, '$.family') SURNAME from postgresqlk8s.public.patient CROSS JOIN UNNEST(CAST(json_extract(resource, '$.name') as 
array(json))) as y(fname) limit 2;

                  ID                  |   SURNAME   
--------------------------------------+-----------
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | Hickle134 
 096724e0-4f46-4af9-8d25-2e5d6ec47526 | Bins636   
(2 rows)


select resource from postgresqlk8s.public.patient CROSS JOIN UNNEST(CAST(json_extract(resource, '$.name') as array(json))) limit 1;
select a.resource from postgresqlk8s.public.patient as a CROSS JOIN UNNEST(CAST(json_extract(resource, '$.name') as array(json))) limit 1;

=====
-> This query appends "name" as a column to c
SELECT a.id ID, json_extract_scalar(c.resource,'$.name[0].family') LNAME FROM postgresqlk8s.public.patient a  INNER JOIN (select id, resource from 
postgresql.public.patient CROSS JOIN UNNEST(CAST(json_extract(resource, '$.name') as array(json)))) as c ON a.id = c.id limit 1;
                  ID                  |   LNAME   
--------------------------------------+-----------
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | Hickle134 
(1 row)

=================== *********** ==============
SELECT a.id ID, json_extract(c.resource,'$.name') NAME,json_extract(json_format(CAST(json_array_get(json_extract(c.resource,'$.name'),0) as json)), '$.family'), 
c.resource FROM postgresqlk8s.public.patient a INNER JOIN (select * from postgresql.public.patient) as c ON a.id = c.id limit 1;
                  ID                  |                                      NAME                                      |    _col2    |                                                                             
>
--------------------------------------+--------------------------------------------------------------------------------+-------------+----------------------------------------------------------------------------->
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | [{"family":"Hickle134","given":["Abram53"],"prefix":["Mr."],"use":"official"}] | "Hickle134" | {"address":[{"city":"Brockton","country":"US","extension":[{"extension":[{"u>
(1 row)

 ====
SELECT a.id ID, b.*, c.resource 
FROM postgresqlk8s.public.patient a 
CROSS JOIN UNNEST(CAST(json_extract(resource, '$.name') as array(json))) as b 
INNER JOIN (select * from postgresql.public.patient) as c ON a.id = c.id limit 1;

>                   ID                  |                                    _col1                                     |                                                                                             
>
--------------------------------------+------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------->
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | {"family":"Hickle134","given":["Abram53"],"prefix":["Mr."],"use":"official"} | {"address":[{"city":"Brockton","country":"US","extension":[{"extension":[

=======
SELECT a.id ID, json_extract(c.resource,'$.name') NAME, c.resource 
FROM postgresqlk8s.public.patient a 
INNER JOIN (select * from postgresql.public.patient) as c 
ON 
a.id = c.id limit 1;

>SELECT a.id ID, json_extract(c.resource,'$.name') NAME, c.resource
            FROM postgresqlk8s.public.patient a
            INNER JOIN (select * from postgresql.public.patient) as c
            ON
            a.id = c.id limit 1;

                  ID                  |                                      NAME                                      |                                                                                           
>
--------------------------------------+--------------------------------------------------------------------------------+------------------------------------------------------------------------------------------->
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | [{"family":"Hickle134","given":["Abram53"],"prefix":["Mr."],"use":"official"}] | {"address":[{"city":"Brockton","country":"US","extension":[{"extension":[{"url":"latitude">
(1 row)

====
SELECT a.id ID, json_array_get(json_extract(c.resource,'$.name'),0)  NAME, json_extract(c.resource, '$.address') FROM postgresqlk8s.public.patient a INNER JOIN 
(select * from postgresql.public.patient) as c ON a.id = c.id limit 1;
                  ID                  |                                     NAME                                     |                                                                                             
>
--------------------------------------+------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------->
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | {"family":"Hickle134","given":["Abram53"],"prefix":["Mr."],"use":"official"} | [{"city":"Brockton","country":"US","extension":[{"extension":[{"url":"latitude","valueDecima>
(1 row)

====
SELECT a.id ID, json_extract_scalar(c.resource,'$.name[0].family')  NAME, json_extract(c.resource, '$.address') FROM postgresqlk8s.public.patient a INNER JOIN
(select * from postgresql.public.patient) as c ON a.id = c.id limit 1;

>                  ID                  |    NAME     |                                                                                                                                                  
_col2       >
--------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------->
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | "Hickle134" | [{"city":"Brockton","country":"US","extension":[{"extension":[{"url":"latitude","valueDecimal":-71.024638},{"url":"longitude","valueDecimal":42.082543}],"url>
(1 row)


===
Alternatively,
presto:public> SELECT a.id ID, json_extract(json_format(CAST(json_array_get(json_extract(c.resource,'$.name'),0) as json)), '$.family') NAME FROM 
postgresqlk8s.public.patient a INNER JOIN (select * from postgresql.public.patient) as c ON a.id = c.id limit 1;
                  ID                  |    NAME     
--------------------------------------+-------------
 d3af67c9-0c02-45f2-bc91-fea45af3ee83 | "Hickle134" 
(1 row)

