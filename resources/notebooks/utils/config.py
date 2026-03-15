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
        },
        
        "prod": {
            # Storage Account
            "storage_account": "streamlineprodadls",
            
            # ADLS Paths
            "bronze_path": "abfss://bronze@streamlineprodadls.dfs.core.windows.net/",
            "silver_path": "abfss://silver@streamlineprodadls.dfs.core.windows.net/",
            "gold_path": "abfss://gold@streamlineprodadls.dfs.core.windows.net/",
            "checkpoint_path": "abfss://checkpoints@streamlineprodadls.dfs.core.windows.net/",
            
            # Unity Catalog
            "catalog": "streamline",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
            
            # Bronze Tables
            "bronze_orders": "streamline.bronze.orders",
            "bronze_payments": "streamline.bronze.payments",
            "bronze_clickstream": "streamline.bronze.clickstream",
            
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
        }
    }
    
    return configs[env]