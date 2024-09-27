from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_quality_check',
    default_args=default_args,
    description='A DAG to perform data quality checks on specified tables',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
) as dag:

    
    sql_query_1 = """
        
        INSERT INTO `dataengineering-434401.Amazonstagingarea.Data_Quality_Log` 
        (check_type, status, table_name, timestamp, record_count, failed_records, error_details)
        VALUES 
        ('Duplicate Check', 
        CASE 
            WHEN (SELECT COUNT(*) 
                  FROM (SELECT product_id 
                        FROM `dataengineering-434401.Amazonstagingarea.Dim_Product`
                        GROUP BY product_id
                        HAVING COUNT(*) > 1) AS dup_check) > 0 THEN 'FAIL' 
            ELSE 'PASS' 
        END, 
        'Dim_Product', 
        CURRENT_TIMESTAMP,  
        (SELECT COUNT(*) FROM `dataengineering-434401.Amazonstagingarea.Dim_Product`), 
        (SELECT COUNT(*) 
         FROM (SELECT product_id 
               FROM `dataengineering-434401.Amazonstagingarea.Dim_Product`
               GROUP BY product_id
               HAVING COUNT(*) > 1) AS failed_count),
        (SELECT STRING_AGG(product_id, ', ') 
         FROM (
             SELECT product_id 
             FROM `dataengineering-434401.Amazonstagingarea.Dim_Product`
             GROUP BY product_id
             HAVING COUNT(*) > 1
         ))
        );
    """

    Duplicate_DIm_product = BigQueryInsertJobOperator(
        task_id='Duplicate_DIm_product',
        job_id='Null check in Dim_Product',  
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,  
            }
        },
        gcp_conn_id='gcp' 
    )

    sql_query_2 = """
        -- Step: Log the results of the rating range check
        INSERT INTO `dataengineering-434401.Amazonstagingarea.Data_Quality_Log` 
        (check_type, status, table_name, timestamp, record_count, failed_records, error_details)
        VALUES 
        (
            'Rating Range Check', 
            CASE 
                WHEN (SELECT COUNT(*) 
                      FROM `dataengineering-434401.Amazonstagingarea.Fact_Review`
                      WHERE rating < 0 OR rating > 5) > 0 THEN 'FAIL' 
                ELSE 'PASS' 
            END, 
            'fact_review', 
            CURRENT_TIMESTAMP,  
            (SELECT COUNT(*) FROM `dataengineering-434401.Amazonstagingarea.Fact_Review`), 
            (SELECT COUNT(*) 
             FROM `dataengineering-434401.Amazonstagingarea.Fact_Review` 
             WHERE rating < 0 OR rating > 5),
            (SELECT STRING_AGG(CAST(rating AS STRING), ', ') 
             FROM `dataengineering-434401.Amazonstagingarea.Fact_Review`
             WHERE rating < 0 OR rating > 5)
        );
    """

    Rating_check_fact_review = BigQueryInsertJobOperator(
        task_id='Rating_check_fact_review',
        job_id='Value range check for rating in Fact_Review',  
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,  
            }
        },
        gcp_conn_id='gcp' 
    )

    
    sql_query_3 = """
        -- Step: Log the results of the null check for user_id in dim_user table
        INSERT INTO `dataengineering-434401.Amazonstagingarea.Data_Quality_Log` 
        (check_type, status, table_name, timestamp, record_count, failed_records, error_details)
        VALUES 
        (
            'Null Check (user_id)', 
            CASE 
                WHEN (SELECT COUNT(*) 
                      FROM `dataengineering-434401.Amazonstagingarea.Dim_User` 
                      WHERE user_id IS NULL) > 0 THEN 'FAIL' 
                ELSE 'PASS' 
            END, 
            'dim_user', 
            CURRENT_TIMESTAMP,  
            (SELECT COUNT(*) FROM `dataengineering-434401.Amazonstagingarea.Dim_User`), 
            (SELECT COUNT(*) 
             FROM `dataengineering-434401.Amazonstagingarea.Dim_User` 
             WHERE user_id IS NULL),
            (SELECT STRING_AGG(CAST(user_id AS STRING), ', ') 
             FROM `dataengineering-434401.Amazonstagingarea.Dim_User`  
             WHERE user_id IS NULL)
        );   
    """

    Null_check_Dim_user = BigQueryInsertJobOperator(
        task_id='Null_check_Dim_user',
        job_id='Null check for user_id in Dim_User',  
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,  
            }
        },
        gcp_conn_id='gcp' 
    )

    
    sql_query_4 = """
        -- Step: Log the results of the duplicate check for the dim_user table
        INSERT INTO `dataengineering-434401.Amazonstagingarea.Data_Quality_Log`
        (check_type, status, table_name, timestamp, record_count, failed_records, error_details)
        VALUES 
        (
            'Duplicate Check', 
            CASE 
                WHEN (SELECT COUNT(*) 
                      FROM (SELECT user_id 
                            FROM `dataengineering-434401.Amazonstagingarea.Dim_User`
                            GROUP BY user_id
                            HAVING COUNT(*) > 1)) > 0 THEN 'FAIL' 
                ELSE 'PASS' 
            END, 
            'dim_user', 
            CURRENT_TIMESTAMP,  
            (SELECT COUNT(*) FROM `dataengineering-434401.Amazonstagingarea.Dim_User`), 
            (SELECT COUNT(*) 
             FROM (SELECT user_id 
                   FROM `dataengineering-434401.Amazonstagingarea.Dim_User`
                   GROUP BY user_id
                   HAVING COUNT(*) > 1)),
            (SELECT STRING_AGG(CAST(user_id AS STRING), ', ') 
             FROM (SELECT user_id 
                   FROM `dataengineering-434401.Amazonstagingarea.Dim_User`
                   GROUP BY user_id
                   HAVING COUNT(*) > 1))
        );
    """

    Duplicate_check_Dim_user = BigQueryInsertJobOperator(
        task_id='Duplicate_check_Dim_user',
        job_id='Duplicate check for user_id in Dim_User',  
        configuration={
            "query": {
                "query": sql_query_4,
                "useLegacySql": False,  
            }
        },
        gcp_conn_id='gcp' 
    )
    # SQL Query for pushing data into the production user table
    sql_query_push_user = """
        WITH clean_dim_user AS (
          SELECT DISTINCT user_id, user_name
          FROM `dataengineering-434401.Amazonstagingarea.Dim_User`
        )
        INSERT INTO `dataengineering-434401.AmazonProductionArea.Dim_User` (user_id, user_name)
        SELECT c.*
        FROM clean_dim_user c
        LEFT JOIN `dataengineering-434401.AmazonProductionArea.Dim_User` u
        ON c.user_id = u.user_id
        WHERE u.user_id IS NULL;
    """

    # Task for pushing user table to production
    push_user_to_production = BigQueryInsertJobOperator(
        task_id='push_user_to_production',
        job_id='push_user_to_production_job',
        configuration={
            "query": {
                "query": sql_query_push_user,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='gcp'
    )

    # SQL Query for pushing data into the production product table
    sql_query_push_product = """
        WITH clean_dim_product AS (
          SELECT DISTINCT product_id, product_name, category, about_product, img_link, product_link
          FROM `dataengineering-434401.Amazonstagingarea.Dim_Product`
        )
        INSERT INTO `dataengineering-434401.AmazonProductionArea.Dim_Product`
          (product_id, product_name, category, about_product, img_link, product_link)
        SELECT c.*
        FROM clean_dim_product c
        LEFT JOIN `dataengineering-434401.AmazonProductionArea.Dim_Product` p
        ON c.product_id = p.product_id
        WHERE p.product_id IS NULL;
    """

    # Task for pushing product table to production
    push_product_to_production = BigQueryInsertJobOperator(
        task_id='push_product_to_production',
        job_id='push_product_to_production_job',
        configuration={
            "query": {
                "query": sql_query_push_product,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='gcp'
    )

    # SQL Query for pushing data into the production fact review table
    sql_query_push_fact_review = """
        WITH clean_fact_review AS (
          SELECT DISTINCT review_id, product_id, user_id, discounted_price, actual_price, discount_percentage, 
                          rating, rating_count, review_title, review_content
          FROM `dataengineering-434401.Amazonstagingarea.Fact_Review`
        )
        INSERT INTO `dataengineering-434401.AmazonProductionArea.Fact_Review`
          (review_id, product_id, user_id, discounted_price, actual_price, discount_percentage, rating, 
           rating_count, review_title, review_content)
        SELECT c.*
        FROM clean_fact_review c
        LEFT JOIN `dataengineering-434401.AmazonProductionArea.Fact_Review` p
        ON c.review_id = p.review_id
        WHERE p.review_id IS NULL;
    """

    # Task for pushing fact review table to production
    push_fact_review_to_production = BigQueryInsertJobOperator(
        task_id='push_fact_review_to_production',
        job_id='push_fact_review_to_production_job',
        configuration={
            "query": {
                "query": sql_query_push_fact_review,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='gcp'
    )

    Duplicate_DIm_product >> Rating_check_fact_review >> Null_check_Dim_user >> Duplicate_check_Dim_user \
    >> push_user_to_production >> push_product_to_production >> push_fact_review_to_production
