CREATE DATABASE IF NOT EXISTS data_engineering_db;
CREATE USER IF NOT EXISTS 'data_user'@'%' IDENTIFIED BY 'userpassword';
GRANT ALL PRIVILEGES ON data_engineering_db.* TO 'data_user'@'%';
FLUSH PRIVILEGES;
