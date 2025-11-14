from database.connection import db_connection
import mysql.connector
import logging

class DataStorage:
    def __init__(self):
        self.conn = db_connection.get_connection()
        
    def initialize_database(self):
        """Initialize database tables with OLTP design"""
        try:
            cursor = self.conn.cursor()
            
            # Files table - stores file metadata
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_id VARCHAR(255) PRIMARY KEY,
                    filename VARCHAR(500),
                    file_type VARCHAR(50),
                    file_size BIGINT,
                    source_type VARCHAR(50),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE,
                    content LONGBLOB,
                    metadata JSON
                )
            """)
            
            # Processed data table - stores extracted content (OLAP star schema fact table)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_data (
                    data_id VARCHAR(255) PRIMARY KEY,
                    file_id VARCHAR(255),
                    content_type VARCHAR(50),
                    extracted_text LONGTEXT,
                    word_count INT,
                    char_count INT,
                    file_metadata JSON,
                    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    quality_score FLOAT,
                    FOREIGN KEY (file_id) REFERENCES files(file_id)
                )
            """)
            
            # Analytics results table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analytics_results (
                    analysis_id VARCHAR(255) PRIMARY KEY,
                    data_id VARCHAR(255),
                    analysis_type VARCHAR(100),
                    results JSON,
                    insights LONGTEXT,
                    confidence_score FLOAT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (data_id) REFERENCES processed_data(data_id)
                )
            """)
            
            # Data quality metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    metric_id VARCHAR(255) PRIMARY KEY,
                    file_id VARCHAR(255),
                    check_type VARCHAR(100),
                    check_value FLOAT,
                    threshold FLOAT,
                    status VARCHAR(50),
                    check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (file_id) REFERENCES files(file_id)
                )
            """)
            
            self.conn.commit()
            cursor.close()
            logging.info("Database tables initialized successfully")
            
        except mysql.connector.Error as e:
            logging.error(f"Database initialization error: {e}")
            raise