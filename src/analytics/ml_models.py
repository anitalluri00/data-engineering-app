import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.metrics import silhouette_score, classification_report
from sklearn.model_selection import train_test_split
import joblib
import logging
from database.connection import db_connection
import mysql.connector

class MLModels:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.models = {}
        
    def train_text_clustering(self, texts, n_clusters=5):
        """Train K-means clustering on text data"""
        try:
            # Vectorize texts
            X = self.vectorizer.fit_transform(texts)
            
            # Perform clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            clusters = kmeans.fit_predict(X)
            
            # Calculate silhouette score
            score = silhouette_score(X, clusters)
            
            # Get cluster centers and top terms
            feature_names = self.vectorizer.get_feature_names_out()
            top_terms_per_cluster = []
            
            for i in range(n_clusters):
                center = kmeans.cluster_centers_[i]
                top_indices = center.argsort()[-10:][::-1]
                top_terms = [feature_names[idx] for idx in top_indices]
                top_terms_per_cluster.append(top_terms)
            
            self.models['text_clustering'] = {
                'kmeans': kmeans,
                'vectorizer': self.vectorizer,
                'n_clusters': n_clusters
            }
            
            return {
                'clusters': clusters.tolist(),
                'silhouette_score': score,
                'top_terms_per_cluster': top_terms_per_cluster,
                'model_info': f'KMeans with {n_clusters} clusters'
            }
            
        except Exception as e:
            logging.error(f"Error in text clustering: {e}")
            raise
    
    def train_anomaly_detection(self, features_df):
        """Train isolation forest for anomaly detection"""
        try:
            # Convert features to numpy array
            X = features_df.values
            
            # Train isolation forest
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            anomalies = iso_forest.fit_predict(X)
            
            # Calculate anomaly scores
            scores = iso_forest.decision_function(X)
            
            self.models['anomaly_detection'] = iso_forest
            
            return {
                'anomalies': anomalies.tolist(),
                'anomaly_scores': scores.tolist(),
                'contamination': 0.1,
                'model_info': 'Isolation Forest for anomaly detection'
            }
            
        except Exception as e:
            logging.error(f"Error in anomaly detection: {e}")
            raise
    
    def train_sentiment_analysis(self, texts, labels=None):
        """Basic sentiment analysis using text features"""
        try:
            if labels is None:
                # Use simple rule-based sentiment as fallback
                return self._rule_based_sentiment(texts)
            
            # Vectorize texts
            X = self.vectorizer.fit_transform(texts)
            
            # Train classifier
            clf = RandomForestClassifier(n_estimators=100, random_state=42)
            clf.fit(X, labels)
            
            # Predict
            predictions = clf.predict(X)
            probabilities = clf.predict_proba(X)
            
            self.models['sentiment_analysis'] = {
                'classifier': clf,
                'vectorizer': self.vectorizer
            }
            
            return {
                'predictions': predictions.tolist(),
                'probabilities': probabilities.tolist(),
                'feature_importance': clf.feature_importances_.tolist(),
                'model_info': 'Random Forest for sentiment analysis'
            }
            
        except Exception as e:
            logging.error(f"Error in sentiment analysis: {e}")
            return self._rule_based_sentiment(texts)
    
    def _rule_based_sentiment(self, texts):
        """Rule-based sentiment analysis as fallback"""
        positive_words = {
            'good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic',
            'outstanding', 'superb', 'brilliant', 'awesome', 'perfect'
        }
        negative_words = {
            'bad', 'terrible', 'awful', 'horrible', 'poor', 'disappointing',
            'unacceptable', 'inadequate', 'subpar', 'mediocre'
        }
        
        sentiments = []
        confidence_scores = []
        
        for text in texts:
            words = set(text.lower().split())
            pos_count = len(words.intersection(positive_words))
            neg_count = len(words.intersection(negative_words))
            total = pos_count + neg_count
            
            if total == 0:
                sentiment = 0  # Neutral
                confidence = 0.0
            else:
                sentiment = (pos_count - neg_count) / total
                confidence = min(abs(sentiment) * 2, 1.0)  # Scale to 0-1
            
            sentiments.append(sentiment)
            confidence_scores.append(confidence)
        
        return {
            'predictions': sentiments,
            'confidence_scores': confidence_scores,
            'model_info': 'Rule-based sentiment analysis'
        }
    
    def perform_topic_modeling(self, texts, n_topics=5):
        """Basic topic modeling using TF-IDF and clustering"""
        try:
            # Vectorize with more features for topics
            topic_vectorizer = TfidfVectorizer(max_features=500, stop_words='english')
            X = topic_vectorizer.fit_transform(texts)
            
            # Use K-means for topic discovery
            kmeans = KMeans(n_clusters=n_topics, random_state=42)
            topics = kmeans.fit_predict(X)
            
            # Get top terms for each topic
            feature_names = topic_vectorizer.get_feature_names_out()
            topic_terms = []
            
            for i in range(n_topics):
                center = kmeans.cluster_centers_[i]
                top_indices = center.argsort()[-15:][::-1]
                terms = [feature_names[idx] for idx in top_indices]
                topic_terms.append(terms)
            
            return {
                'topics': topics.tolist(),
                'topic_terms': topic_terms,
                'n_topics': n_topics,
                'model_info': f'Topic modeling with {n_topics} topics'
            }
            
        except Exception as e:
            logging.error(f"Error in topic modeling: {e}")
            raise
    
    def generate_ml_insights(self, file_data):
        """Generate comprehensive ML insights for files"""
        try:
            insights = {}
            
            # Extract texts for analysis
            texts = [data.get('extracted_text', '') for data in file_data]
            valid_texts = [text for text in texts if text and len(text.strip()) > 50]
            
            if len(valid_texts) >= 5:  # Minimum texts for meaningful analysis
                # Text clustering
                clustering_results = self.train_text_clustering(valid_texts, n_clusters=3)
                insights['clustering'] = clustering_results
                
                # Topic modeling
                topic_results = self.perform_topic_modeling(valid_texts, n_topics=3)
                insights['topic_modeling'] = topic_results
                
                # Sentiment analysis
                sentiment_results = self.train_sentiment_analysis(valid_texts)
                insights['sentiment_analysis'] = sentiment_results
                
            # Feature-based analysis
            features = self._extract_ml_features(file_data)
            if len(features) >= 10:  # Minimum for anomaly detection
                anomaly_results = self.train_anomaly_detection(pd.DataFrame(features))
                insights['anomaly_detection'] = anomaly_results
            
            return insights
            
        except Exception as e:
            logging.error(f"Error generating ML insights: {e}")
            return {'error': str(e)}
    
    def _extract_ml_features(self, file_data):
        """Extract features for machine learning"""
        features = []
        
        for data in file_data:
            text = data.get('extracted_text', '')
            metadata = data.get('file_metadata', {})
            
            feature_vector = {
                'word_count': len(text.split()),
                'char_count': len(text),
                'sentence_count': len([s for s in text.split('.') if s.strip()]),
                'avg_word_length': np.mean([len(word) for word in text.split()]) if text else 0,
                'unique_word_ratio': len(set(text.split())) / max(len(text.split()), 1),
                'capital_ratio': sum(1 for char in text if char.isupper()) / max(len(text), 1),
                'digit_ratio': sum(1 for char in text if char.isdigit()) / max(len(text), 1),
                'special_char_ratio': sum(1 for char in text if not char.isalnum() and not char.isspace()) / max(len(text), 1),
            }
            
            features.append(feature_vector)
        
        return features
    
    def save_models(self, filepath):
        """Save trained models to disk"""
        try:
            joblib.dump(self.models, filepath)
            logging.info(f"Models saved to {filepath}")
        except Exception as e:
            logging.error(f"Error saving models: {e}")
    
    def load_models(self, filepath):
        """Load trained models from disk"""
        try:
            self.models = joblib.load(filepath)
            logging.info(f"Models loaded from {filepath}")
        except Exception as e:
            logging.error(f"Error loading models: {e}")