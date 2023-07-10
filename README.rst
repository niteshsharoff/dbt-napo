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

Local Development
-----------------

Setup Virtual Environment:

.. code:: bash

  cd dbt-napo
  python3 -m venv venv
  source venv/bin/activate

Install Requirements:

.. code:: bash

  pip install -r requirements-dev.txt

Install Pre-commit:

.. code:: bash

  pre-commit install

Install DBT Dependencies:

.. code:: bash

  dbt deps

Load Seed Files:

.. code:: bash

  dbt seed

Running A Specific Set of Models & Dependencies:

.. code:: bash

  dbt run --select +marts.reporting+ intermediate.underwriter

Running Tests:

.. code:: bash

  dbt test --select marts.reporting --store-failures