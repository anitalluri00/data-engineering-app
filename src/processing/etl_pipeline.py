import pandas as pd
import logging
from database.connection import db_connection
import mysql.connector
from processing.data_quality import DataQualityChecker

class ETLPipeline:
    def __init__(self):
        self.quality_checker = DataQualityChecker()
    
    def run_pipeline(self, batch_size=100):
        """Run complete ETL pipeline"""
        try:
            # Extract - get unprocessed files
            files_data = self._extract_unprocessed_files(batch_size)
            
            if not files_data:
                logging.info("No unprocessed files found")
                return
            
            # Transform - process data
            transformed_data = self._transform_data(files_data)
            
            # Load - store analytics results
            self._load_analytics(transformed_data)
            
            logging.info(f"ETL pipeline completed for {len(transformed_data)} files")
            
        except Exception as e:
            logging.error(f"ETL pipeline error: {e}")
            raise
    
    def _extract_unprocessed_files(self, batch_size):
        try:
            conn = db_connection.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT f.file_id, f.filename, f.file_type, f.metadata, 
                       pd.data_id, pd.content_type, pd.extracted_text
                FROM files f
                JOIN processed_data pd ON f.file_id = pd.file_id
                WHERE f.processed = TRUE 
                AND NOT EXISTS (
                    SELECT 1 FROM analytics_results ar WHERE ar.data_id = pd.data_id
                )
                LIMIT %s
            """, (batch_size,))
            
            results = cursor.fetchall()
            cursor.close()
            return results
            
        except mysql.connector.Error as e:
            logging.error(f"Error extracting unprocessed files: {e}")
            return []
    
    def _transform_data(self, files_data):
        transformed_data = []
        
        for file_data in files_data:
            try:
                # Data quality checks
                quality_metrics = self.quality_checker.check_data_quality(file_data)
                
                # Basic analytics
                analytics = self._perform_basic_analytics(file_data)
                
                # Feature engineering
                features = self._engineer_features(file_data)
                
                transformed_data.append({
                    'data_id': file_data['data_id'],
                    'quality_metrics': quality_metrics,
                    'analytics': analytics,
                    'features': features,
                    'file_metadata': file_data
                })
                
            except Exception as e:
                logging.error(f"Error transforming data for file {file_data['file_id']}: {e}")
                continue
        
        return transformed_data
    
    def _perform_basic_analytics(self, file_data):
        text = file_data.get('extracted_text', '')
        
        # Basic text analytics
        word_count = len(text.split())
        char_count = len(text)
        sentence_count = len([s for s in text.split('.') if s.strip()])
        
        # Simple sentiment analysis (basic implementation)
        positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful']
        negative_words = ['bad', 'terrible', 'awful', 'horrible', 'poor']
        
        positive_count = sum(1 for word in text.lower().split() if word in positive_words)
        negative_count = sum(1 for word in text.lower().split() if word in negative_words)
        
        sentiment_score = (positive_count - negative_count) / max(word_count, 1)
        
        return {
            'word_count': word_count,
            'char_count': char_count,
            'sentence_count': sentence_count,
            'sentiment_score': sentiment_score,
            'readability_score': self._calculate_readability(text)
        }
    
    def _calculate_readability(self, text):
        # Simple readability score implementation
        words = text.split()
        sentences = [s for s in text.split('.') if s.strip()]
        
        if len(words) == 0 or len(sentences) == 0:
            return 0
            
        avg_sentence_length = len(words) / len(sentences)
        return max(0, 100 - avg_sentence_length)
    
    def _engineer_features(self, file_data):
        text = file_data.get('extracted_text', '')
        
        return {
            'has_numbers': any(char.isdigit() for char in text),
            'has_special_chars': any(not char.isalnum() for char in text),
            'avg_word_length': sum(len(word) for word in text.split()) / max(len(text.split()), 1),
            'capital_ratio': sum(1 for char in text if char.isupper()) / max(len(text), 1)
        }
    
    def _load_analytics(self, transformed_data):
        try:
            conn = db_connection.get_connection()
            cursor = conn.cursor()
            
            for data in transformed_data:
                cursor.execute("""
                    INSERT INTO analytics_results 
                    (analysis_id, data_id, analysis_type, results, insights, confidence_score)
                    VALUES (UUID(), %s, %s, %s, %s, %s)
                """, (
                    data['data_id'],
                    'basic_analytics',
                    str(data['analytics']),
                    f"Processed file with {data['analytics']['word_count']} words",
                    data['analytics'].get('sentiment_score', 0.5)
                ))
                
                # Store quality metrics
                for metric_type, metric_value in data['quality_metrics'].items():
                    cursor.execute("""
                        INSERT INTO data_quality_metrics 
                        (metric_id, file_id, check_type, check_value, status)
                        VALUES (UUID(), %s, %s, %s, %s)
                    """, (
                        data['file_metadata']['file_id'],
                        metric_type,
                        metric_value.get('value', 0),
                        metric_value.get('status', 'unknown')
                    ))
            
            conn.commit()
            cursor.close()
            
        except mysql.connector.Error as e:
            logging.error(f"Error loading analytics data: {e}")
            raise