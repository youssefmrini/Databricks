# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.name = 'access-data-externally'
lesson_config.create_schema = False
lesson_config.create_catalog = False
lesson_config.require_uc = False
lesson_config.installing_datasets = False

DA = DBAcademyHelper(course_config, lesson_config)  # Create the DA object
DA.reset_lesson()                                   # Reset the lesson to a clean state
DA.init()                                           # Performs basic intialization including creating schemas and catalogs
dbutils.fs.mkdirs(DA.paths.working_dir)
DA.conclude_setup()                                 # Finalizes the state and prints the config for the student
