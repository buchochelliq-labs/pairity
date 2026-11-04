# Databricks notebook source
from pairity.job_tasks import compare

compare(spark, dbutils)  # noqa: F821
