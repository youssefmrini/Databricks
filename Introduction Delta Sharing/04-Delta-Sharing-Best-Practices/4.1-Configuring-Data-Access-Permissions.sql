-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Sharing Best Practices: Configuring Data Access Permissions
-- MAGIC 
-- MAGIC In this demo, we are going to discuss Delta Sharing best practices that will allow you to set up appropriate security controls and data access auditing when sharing data. In the first section of the course, we are going to demonstrate how to manage data access permissions. Then, we are are going to discuss IP access lists and how to configure them for a recipient.
-- MAGIC 
-- MAGIC **Learning Objectives:**
-- MAGIC 
-- MAGIC - Describe data access permissions for Delta Sharing
-- MAGIC 
-- MAGIC - Manage recipient privileges on shares
-- MAGIC 
-- MAGIC - Describe use cases of IP access list
-- MAGIC 
-- MAGIC - Configure IP access list

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC As Delta Sharing is integrated with Unity Catalog (UC), there are couple prerequisites that need you need to ensure before starting this course.
-- MAGIC 
-- MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
-- MAGIC 
-- MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
-- MAGIC 
-- MAGIC - Only a metastore admin or account admin can share data using Delta Sharing. Therefore, make sure that you have necessary permissions to share data.
-- MAGIC 
-- MAGIC - **External data sharing must be enabled** for your account. The external data sharing enablement process is covered in the "Overview of Delta Sharing" course. 
-- MAGIC 
-- MAGIC - The minimum DBR version required is **DBR 11.2**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Classroom Setup
-- MAGIC 
-- MAGIC 
-- MAGIC The first thing we're going to do is to run a setup script. This script will define the required configuration variables that are scoped to each user.
-- MAGIC 
-- MAGIC This script will create a catalog and a schema and import a sample dataset into the schema.  

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Dataset Details
-- MAGIC 
-- MAGIC Run the code block below to view necessary details that we will need in this course. Note the catalog name, database name and table for the sample data that we are going to share using Delta Sharing.
-- MAGIC 
-- MAGIC In addition, as you progress through this course, you will see various references to the object **`DA`**. This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricks API. For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here below. In this course we are going to use UI mostly, therefore, we are not going to need them for now.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Username:          {DA.username}")
-- MAGIC print(f"Working Directory: {DA.paths.working_dir}")
-- MAGIC print(f"Catalog Name:      {DA.catalog_name}")
-- MAGIC print(f"Schema Name:       {DA.database_name}")
-- MAGIC print(f"Table Name:        {DA.table_name}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Data Access Management
-- MAGIC 
-- MAGIC Databricks supports data access management via Data Explorer UI, Databricks Unity Catalog CLI and SQL commands. In this course, we are going to use the UI for configuring data access permissions. You can do the same steps with CLI or SQL commands too. You can refer to "Share Data within Databricks Using Delta Sharing" course which covers most SQL commands for Delta Sharing. 
-- MAGIC 
-- MAGIC In this section, first, we are going to create a share and a recipient. Then, we are going to show how grant and revoke access for the recipient.
-- MAGIC 
-- MAGIC ### Create a Share
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - On the top right, click **Share Data** button.
-- MAGIC 
-- MAGIC - Enter *"flights_data_share"* as name of the share.
-- MAGIC 
-- MAGIC - Enter any description you want in the comment section.
-- MAGIC 
-- MAGIC - Click **Create** button.
-- MAGIC 
-- MAGIC 
-- MAGIC ### Create a Recipient
-- MAGIC 
-- MAGIC To create a *New recipient*;
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - Click **New recipient** button.
-- MAGIC 
-- MAGIC - Enter *"flights_data_recipient"* as the name of new recipient. In this example, we will create an "external recipient", therefore, leave the  **Sharing Identifier** box empty.
-- MAGIC 
-- MAGIC - Enter a description you want to define for the recipient in Comment box.
-- MAGIC 
-- MAGIC - Click **Create** button.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Grant Recipient Access to the Share
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - Click on the **Recipient Name** (*flights_data_recipient*). 
-- MAGIC 
-- MAGIC - Click **Grant share**.
-- MAGIC 
-- MAGIC - On the Grant share dialog, start typing the share name or click the drop-down menu to select the share (*"flights_data_share"*) we created before. 
-- MAGIC 
-- MAGIC - Click **Grant**.
-- MAGIC 
-- MAGIC 
-- MAGIC ### View Grants on a Share
-- MAGIC 
-- MAGIC It is important to track who has access to a share. To view recipients who have access to a share;
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - On the **Shares tab**, find and select the share.
-- MAGIC 
-- MAGIC - Go to the **Recipients tab** to view all recipients who have access to the share.
-- MAGIC 
-- MAGIC ### View Grants of a Recipient
-- MAGIC 
-- MAGIC Another way of inspecting access to shared data is to view recipient grants. This method can be useful if you want to make sure a user does not have access to the data that is not supposed to be accessed by the user.
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - On the **Recipients tab**, find and select the share.
-- MAGIC 
-- MAGIC - Go to the **Shares tab** to view all shares that teh recipient has access to.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Revoke Recipient Access to a Share
-- MAGIC 
-- MAGIC We can revoke access from Recipient or Share page. In this example, let's do that from the Share.
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - On the **Shares tab**, find and select the share.
-- MAGIC 
-- MAGIC - Go to the **Recipients tab** to view all recipients who have access to the share.
-- MAGIC 
-- MAGIC - Click the three-dot menu and select **Revoke**.
-- MAGIC 
-- MAGIC - On the confirmation dialog, click **Revoke**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Configuring IP Access List
-- MAGIC 
-- MAGIC IP Access List can be configured for  external (token-managed) Delta Sharing recipients only. As Databricks-to-Databricks sharing is governed by Databricks Control Plane, there is no need for network level restrictions. 
-- MAGIC 
-- MAGIC Before moving to the next steps, make sure that you have at least one external recipient in your workspace. If you want to create a new external recipient, you can watch  **“Share and Access Data Externally”** course which covers all aspects of external data sharing. 
-- MAGIC 
-- MAGIC Databricks supports two methods for configuring IP Access List: **Data Explorer UI** and **Databricks Unity Catalog CLI**. In this demo, we are going to use the UI. If you want to follow the same steps using CLI, you can refer to [the documentation page to learn more about CLI commands for adding an IP access list](https://docs.databricks.com/data-sharing/access-list.html#use-ip-access-lists-to-restrict-delta-sharing-recipient-access-open-sharing).
-- MAGIC 
-- MAGIC **📌 Note: You must be recipient object owner to add IP access list for a recipient.**
-- MAGIC 
-- MAGIC To configure IP Access List for an external recipient:
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - View **Recipients** tab. For each recipient, you will see **Authentication Type** field which is defined as **TOKEN** or **DATABRICKS.**
-- MAGIC 
-- MAGIC - Click on the **Recipient Name** (*flights_data_recipient*). 
-- MAGIC 
-- MAGIC - Click on **IP access list** tab. This screen will list all allowed IP addresses/CIDRs.
-- MAGIC 
-- MAGIC - Click on **Add IP address/CIDRs** to add a new allowed IP range.
-- MAGIC 
-- MAGIC - Add an IP address or IP address range.
-- MAGIC     - For single IP address use x.x.x.x format
-- MAGIC     - For IP address range, use CIDR format like x.x.x.x/x
-- MAGIC 
-- MAGIC - You can remove a recipient’s IP access list by clicking the trash can icon next to the IP address you want to delete. **If you remove all IP addresses from the list, the recipient can access the shared data from anywhere.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Clean up Classroom
-- MAGIC 
-- MAGIC > 📌 **IMPORTANT: Running the code below will delete the catalog and tables used for data sharing. You should run this code block after completing next lesson as you will need the share and the recipient we created here.**
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # remove shares and recipients
-- MAGIC spark.sql("DROP SHARE IF EXISTS flights_data_share")
-- MAGIC spark.sql("DROP RECIPIENT IF EXISTS flights_data_recipient")
-- MAGIC 
-- MAGIC # Remove lesson resourses
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC In this course, we covered Delta Sharing best practices that will allow you to set up appropriate security controls. First, we demonstrated how to grant and revoke access to a share. Then, we showed how to configure IP access list for the recipient.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
