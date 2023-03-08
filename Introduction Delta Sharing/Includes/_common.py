# Databricks notebook source
# MAGIC %run ./_libraries

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(course_code = "itds",                  # The abbreviated version of the course (4 chars preferred)
                             course_name = "introduction-to-delta-sharing",        # The full name of the course, hyphenated
                             data_source_name = "introduction-to-delta-sharing",   # Should be the same as the course
                             data_source_version = "v01",           # New courses would start with 01
                             install_min_time = "1 min",            # The minimum amount of time to install the datasets (e.g. from Oregon)
                             install_max_time = "5 min",            # The maximum amount of time to install the datasets (e.g. from India)
                             remote_files = remote_files,           # The enumerated list of files in the datasets
                             supported_dbrs = ["11.2.x-scala2.12", "11.2.x-photon-scala2.12", "11.3.x-scala2.12", "11.3.x-photon-scala2.12"],
                             expected_dbrs = "11.3.x-scala2.12, 11.3.x-photon-scala2.12, 11.2.x-scala2.12, 11.2.x-photon-scala2.12")

# Defined here for the majority of lessons, 
# and later modified on a per-lesson basis.
lesson_config = LessonConfig(name = "introduction-to-delta-sharing",                           # The name of the course - used to cary state between notebooks
                             create_schema = False,                  # True if the user-specific schama (database) should be created
                             create_catalog = True,                # Requires UC, but when True creates the user-specific catalog
                             requires_uc = True,                   # Indicates if this course requires UC or not
                             installing_datasets = True,            # Indicates that the datasets should be installed or not
                             enable_streaming_support = False      # Indicates that this lesson uses streaming (e.g. needs a checkpoint directory)
                            )      
