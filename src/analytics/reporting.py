import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
from database.connection import db_connection
import mysql.connector

class AnalyticsReporter:
    def generate_dashboard_data(self):
        """Generate comprehensive analytics dashboard data"""
        try:
            conn = db_connection.get_connection()
            
            # File type distribution
            file_type_df = self._get_file_type_distribution(conn)
            
            # Processing statistics
            processing_stats = self._get_processing_stats(conn)
            
            # Data quality overview
            quality_overview = self._get_quality_overview(conn)
            
            # Analytics insights
            analytics_insights = self._get_analytics_insights(conn)
            
            return {
                'file_type_distribution': file_type_df,
                'processing_stats': processing_stats,
                'quality_overview': quality_overview,
                'analytics_insights': analytics_insights
            }
            
        except Exception as e:
            logging.error(f"Error generating dashboard data: {e}")
            return {}
    
    def _get_file_type_distribution(self, conn):
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT file_type, COUNT(*) as count 
            FROM files 
            GROUP BY file_type 
            ORDER BY count DESC
        """)
        results = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(results)
    
    def _get_processing_stats(self, conn):
        cursor = conn.cursor(dictionary=True)
        
        # Total files
        cursor.execute("SELECT COUNT(*) as total_files FROM files")
        total_files = cursor.fetchone()['total_files']
        
        # Processed files
        cursor.execute("SELECT COUNT(*) as processed_files FROM files WHERE processed = TRUE")
        processed_files = cursor.fetchone()['processed_files']
        
        # Files with analytics
        cursor.execute("""
            SELECT COUNT(DISTINCT f.file_id) as analyzed_files 
            FROM files f 
            JOIN processed_data pd ON f.file_id = pd.file_id 
            JOIN analytics_results ar ON pd.data_id = ar.data_id
        """)
        analyzed_files = cursor.fetchone()['analyzed_files']
        
        cursor.close()
        
        return {
            'total_files': total_files,
            'processed_files': processed_files,
            'analyzed_files': analyzed_files,
            'processing_rate': (processed_files / total_files * 100) if total_files > 0 else 0
        }
    
    def _get_quality_overview(self, conn):
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT check_type, status, COUNT(*) as count 
            FROM data_quality_metrics 
            GROUP BY check_type, status
        """)
        results = cursor.fetchall()
        cursor.close()
        
        quality_data = {}
        for row in results:
            if row['check_type'] not in quality_data:
                quality_data[row['check_type']] = {}
            quality_data[row['check_type']][row['status']] = row['count']
        
        return quality_data
    
    def _get_analytics_insights(self, conn):
        cursor = conn.cursor(dictionary=True)
        
        # Average metrics
        cursor.execute("""
            SELECT 
                AVG(JSON_EXTRACT(results, '$.word_count')) as avg_word_count,
                AVG(JSON_EXTRACT(results, '$.sentiment_score')) as avg_sentiment,
                AVG(confidence_score) as avg_confidence
            FROM analytics_results
        """)
        averages = cursor.fetchone()
        
        # Recent activity
        cursor.execute("""
            SELECT DATE(created_date) as date, COUNT(*) as count
            FROM analytics_results
            WHERE created_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY DATE(created_date)
            ORDER BY date DESC
        """)
        recent_activity = cursor.fetchall()
        
        cursor.close()
        
        return {
            'averages': averages,
            'recent_activity': recent_activity
        }