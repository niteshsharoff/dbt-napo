Data Engineering
================

.. contents::


Introduction
------------

Nitesh and Oisin are working on early process to generate key metrics from policy and other data. This is starting with an effort to replicate what we have in Heap using Metabase, BigQuery and data exported from the policy service. 

- https://www.notion.so/napopetinsurance/Business-Performance-Reporting-67f55559607b4e60915ca1b4efcabd88


Two folders added:
----------------
* gcp-pq-to-bq-loader
  -This is the loader for cloud storage to BigQuery
* dbt-napo
  -This is a version controlled transformation layer on the raw BigQuery dataset