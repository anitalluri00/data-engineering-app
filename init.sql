-- Create database and user
CREATE DATABASE IF NOT EXISTS data_engineering_db;
CREATE USER IF NOT EXISTS 'data_user'@'%' IDENTIFIED BY 'userpassword';
GRANT ALL PRIVILEGES ON data_engineering_db.* TO 'data_user'@'%';
FLUSH PRIVILEGES;

-- Use the database
USE data_engineering_db;

-- Create tables (these will also be created by the application, but this ensures they exist)
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
);

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
);

CREATE TABLE IF NOT EXISTS analytics_results (
    analysis_id VARCHAR(255) PRIMARY KEY,
    data_id VARCHAR(255),
    analysis_type VARCHAR(100),
    results JSON,
    insights LONGTEXT,
    confidence_score FLOAT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_id) REFERENCES processed_data(data_id)
);

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id VARCHAR(255) PRIMARY KEY,
    file_id VARCHAR(255),
    check_type VARCHAR(100),
    check_value FLOAT,
    threshold FLOAT,
    status VARCHAR(50),
    check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_id) REFERENCES files(file_id)
);