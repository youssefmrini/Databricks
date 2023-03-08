-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Sharing Best Practices: Manage Recipient Tokens
-- MAGIC 
-- MAGIC In this section of the course we are going to discuss the role of recipient tokens and how to manage them. Tokens are used for open-source (token-based connection) type of Delta Sharing. If a recipient has access to a Databricks workspace that is enabled for Unity Catalog, we can use Databricks-to-Databricks sharing, and no token-based credentials are required. Recipient's **sharing identifier** can be used to establish the secure connection.
-- MAGIC 
-- MAGIC **Learning Objectives:**
-- MAGIC 
-- MAGIC - Explain the best practices for token management
-- MAGIC 
-- MAGIC - Define token lifetime and its importance
-- MAGIC 
-- MAGIC - Rotate credential tokens based on security best-practices

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
-- MAGIC - The minimum DBR version required is **DBR 11.2**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Manage Recipient Tokens
-- MAGIC 
-- MAGIC Token management involves two main tasks; 
-- MAGIC 
-- MAGIC - defining the default token lifetime
-- MAGIC - rotating credentials when needed.
-- MAGIC 
-- MAGIC Token management is applicable for external (token-managed) recipients only. As internal (Databricks-to-Databricks) credentials are managed by Databricks, you don’t need to manage tokens.
-- MAGIC 
-- MAGIC Before moving to the next steps, make sure that you have at least one external recipient in your workspace. We have created a new external recipient in the previous lesson, which you can use for this lesson as well. If you want to learn more about the process of creating a new external recipient, you can watch **“Share and Access Data Externally”** course which covers all aspects of external data sharing.
-- MAGIC 
-- MAGIC Databricks supports two methods for managing recipient tokens: **Data Explorer UI** and **Databricks Unity Catalog CLI**. In this demo, we are going to use the UI. If you want to follow the same steps using CLI, [you can refer to the documentation page to learn more about CLI commands for managing recipient tokens](https://docs.databricks.com/data-sharing/create-recipient.html#manage-recipient-tokens-open-sharing).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Recipient Token Lifetime
-- MAGIC 
-- MAGIC To modify recipient token lifetime;
-- MAGIC 
-- MAGIC - Login to **Account Console**.
-- MAGIC 
-- MAGIC - In the left sidebar, click **Data**.
-- MAGIC 
-- MAGIC - Click the **metastore name** that you want to modify the token lifetime.
-- MAGIC 
-- MAGIC - In **Configuration** tab, click **Edit** under “Delta Sharing recipient token lifetime” option.
-- MAGIC 
-- MAGIC - Enable **Set expiration**. Enter a number of seconds, minutes, hours, or days, and select the unit of measure.
-- MAGIC 
-- MAGIC - Click **Save**.
-- MAGIC 
-- MAGIC 📌 If you disable **Set expiration or enter 0 as token lifetime,** recipient tokens do not expire. Databricks recommends that you configure tokens to expire.
-- MAGIC 
-- MAGIC 📌 The recipient token lifetime for existing recipients **is not updated automatically** when you change the default recipient token lifetime for a metastore. In order to apply the new token lifetime to a given recipient, you must rotate their token.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rotate a Recipient’s Token
-- MAGIC 
-- MAGIC There are a number of reasons that you might want to rotate credentials;
-- MAGIC 
-- MAGIC - To update the recipient’s token lifetime after you modify the recipient token lifetime for a metastore.
-- MAGIC 
-- MAGIC - When the existing recipient token is about to expire.
-- MAGIC 
-- MAGIC - If a recipient loses their activation URL or if it is compromised.
-- MAGIC 
-- MAGIC - If the credential file is corrupted, lost, or compromised after it is downloaded by a recipient.
-- MAGIC 
-- MAGIC To rotate a recipient’s token, you can use Data Explorer or the Databricks Unity Catalog CLI. In this demo, we are going to use the UI.
-- MAGIC 
-- MAGIC **📌 Note: You must be recipient object owner to rotate the token.**
-- MAGIC 
-- MAGIC In this demo, let’s consider if a recipient’s credentials are compromised and we need to reset the credentials. Following steps are required to rotate a recipient’s token and generate a new token.
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - View **Recipients** tab. For each recipient, you will see **Authentication Type** field which is defined as **TOKEN** or **DATABRICKS.** Token lifetime is applicable to token-managed recipients only.
-- MAGIC 
-- MAGIC - Click on the Recipient **Name.**
-- MAGIC 
-- MAGIC - On the **Details** tab, under **Token Expiration**, click **Rotate**.
-- MAGIC 
-- MAGIC - On the Rotate token dialog, set the token to expire either immediately or for a set period of time. In this scenario as the recipient token is compromised, Databricks recommends to force the existing recipient token to expire immediately. Thus, let’s select **Immediately** for expiration period.
-- MAGIC 
-- MAGIC - Click **Rotate** button.
-- MAGIC 
-- MAGIC **This process will reset recipient’s token and generate a new activation link** that can be used for obtaining a new token. 
-- MAGIC 
-- MAGIC - On the Recipient **Details** tab, **copy the new Activation link** and share it with the recipient over a secure channel.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC In this section of the course, we presented the best practices for token management. Tokens are very important for security in token-managed connection type. Token management typically involves two main tasks; defining token lifetime and rotating token for existing recipients. We showed how to define the token lifetime and showed how to rotate a recipient's token when needed. 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
