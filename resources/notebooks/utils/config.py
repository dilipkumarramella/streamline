def get_config(env: str) -> dict:
    
    configs = {
        "dev": {
            # Storage Account
            "storage_account": "streamlinedevstorage",
            
            # ADLS Paths
            "bronze_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/bronze/",
            "silver_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/silver/",
            "gold_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/gold/",
            "checkpoint_path": "abfss://streamline@streamlinedevstorage.dfs.core.windows.net/checkpoints/",
            
            # Unity Catalog
            "catalog": "streamline",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
            
            # Bronze Tables
            "bronze_orders": "streamline.bronze.orders",
            "bronze_payments": "streamline.bronze.payments",
            "bronze_clickstream": "streamline.bronze.clickstream",
            "bronze_dead_letter": "streamline.bronze.dead_letter",
            
            # Silver Tables
            "silver_fact_orders": "streamline.silver.fact_orders",
            "silver_fact_payments": "streamline.silver.fact_payments",
            "silver_fact_events": "streamline.silver.fact_events",
            "silver_dim_customer": "streamline.silver.dim_customer",
            "silver_dim_product": "streamline.silver.dim_product",
            "silver_orders_quarantine": "streamline.silver.orders_quarantine",
            "silver_payments_quarantine": "streamline.silver.payments_quarantine",
            
            # Gold Tables
            "gold_orders_daily": "streamline.gold.orders_daily_summary",
            "gold_customer_360": "streamline.gold.customer_360",
            "gold_funnel_metrics": "streamline.gold.funnel_metrics",
            "gold_payment_success": "streamline.gold.payment_success_rate",
            "gold_data_quality": "streamline.gold.data_quality_metrics",

            # Pipeline State
            "pipeline_state": "streamline.silver.pipeline_state",

            # Pool tables
            "customer_pool": "streamline.bronze.customer_pool",
            "product_pool": "streamline.bronze.product_pool",

            # Kafka config
            "KAFKA_BOOTSTRAP_SERVERS": "pkc-41p56.asia-south1.gcp.confluent.cloud:9092",
            "KAFKA_API_KEY": "MG7IHRPFZWE3KAV4",
            "KAFKA_API_SECRET": "cfltHZgMnMXr/zPSd2wPrf/TFOvD5uElcb9kvN4U1iaHzS9btibgfGCov8MTPsDw",
            "KAFKA_TOPIC_ORDERS": "orders",
            "KAFKA_TOPIC_CLICKSTREAM": "clickstream",
            "KAFKA_TOPIC_PAYMENTS": "payments"
        },
        
        "prod": {
            # Storage Account
            "storage_account": "streamlineprdstorage",
            
            # ADLS Paths
            "bronze_path": "abfss://streamline@streamlineprdstorage.dfs.core.windows.net/bronze/",
            "silver_path": "abfss://streamline@streamlineprdstorage.dfs.core.windows.net/silver/",
            "gold_path": "abfss://streamline@streamlineprdstorage.dfs.core.windows.net/gold/",
            "checkpoint_path": "abfss://streamline@streamlineprdstorage.dfs.core.windows.net/checkpoints",
            
            # Unity Catalog
            "catalog": "streamline",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
            
            # Bronze Tables
            "bronze_orders": "streamline.bronze.orders",
            "bronze_payments": "streamline.bronze.payments",
            "bronze_clickstream": "streamline.bronze.clickstream",
            "bronze_dead_letter": "streamline.bronze.dead_letter",
            
            # Silver Tables
            "silver_fact_orders": "streamline.silver.fact_orders",
            "silver_fact_payments": "streamline.silver.fact_payments",
            "silver_fact_events": "streamline.silver.fact_events",
            "silver_dim_customer": "streamline.silver.dim_customer",
            "silver_dim_product": "streamline.silver.dim_product",
            "silver_orders_quarantine": "streamline.silver.orders_quarantine",
            "silver_payments_quarantine": "streamline.silver.payments_quarantine",
            
            # Gold Tables
            "gold_orders_daily": "streamline.gold.orders_daily_summary",
            "gold_customer_360": "streamline.gold.customer_360",
            "gold_funnel_metrics": "streamline.gold.funnel_metrics",
            "gold_payment_success": "streamline.gold.payment_success_rate",
            "gold_data_quality": "streamline.gold.data_quality_metrics",

            # Pipeline State
            "pipeline_state": "streamline.silver.pipeline_state",

            # Pool tables
            "customer_pool": "streamline.bronze.customer_pool",
            "product_pool": "streamline.bronze.product_pool",

            # Kafka config
            "KAFKA_BOOTSTRAP_SERVERS": "pkc-41p56.asia-south1.gcp.confluent.cloud:9092",
            "KAFKA_API_KEY": "MG7IHRPFZWE3KAV4",
            "KAFKA_API_SECRET": "cfltHZgMnMXr/zPSd2wPrf/TFOvD5uElcb9kvN4U1iaHzS9btibgfGCov8MTPsDw",
            "KAFKA_TOPIC_ORDERS": "orders",
            "KAFKA_TOPIC_CLICKSTREAM": "clickstream",
            "KAFKA_TOPIC_PAYMENTS": "payments"
        }
    }
    
    return configs[env]