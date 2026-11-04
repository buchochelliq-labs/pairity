# Databricks notebook source
from pairity.job_tasks import prepare

prepare(spark, dbutils)  # noqa: F821
