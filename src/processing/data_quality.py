import logging

class DataQualityChecker:
    def check_data_quality(self, file_data):
        """Comprehensive data quality checks"""
        checks = {}
        
        try:
            text = file_data.get('extracted_text', '')
            
            # Completeness check
            checks['completeness'] = self._check_completeness(text)
            
            # Validity check
            checks['validity'] = self._check_validity(text)
            
            # Consistency check
            checks['consistency'] = self._check_consistency(file_data)
            
            # Uniqueness check (basic)
            checks['uniqueness'] = self._check_uniqueness(text)
            
            # Accuracy check (basic)
            checks['accuracy'] = self._check_accuracy(file_data)
            
        except Exception as e:
            logging.error(f"Data quality check error: {e}")
            checks['error'] = {'value': 0, 'status': 'failed'}
        
        return checks
    
    def _check_completeness(self, text):
        """Check if data is complete"""
        if not text or text.strip() == "":
            return {'value': 0, 'status': 'failed'}
        
        # Check if text has meaningful content
        word_count = len(text.split())
        if word_count < 10:
            return {'value': 0.3, 'status': 'poor'}
        elif word_count < 50:
            return {'value': 0.6, 'status': 'fair'}
        else:
            return {'value': 0.9, 'status': 'good'}
    
    def _check_validity(self, text):
        """Check if data is valid"""
        # Basic validity checks
        if len(text) > 1000000:  # Too large
            return {'value': 0.2, 'status': 'poor'}
        
        # Check for common invalid patterns
        invalid_patterns = ['�', 'NULL', 'undefined']
        invalid_count = sum(text.count(pattern) for pattern in invalid_patterns)
        
        score = max(0, 1 - (invalid_count / max(len(text.split()), 1)))
        status = 'good' if score > 0.8 else 'poor'
        
        return {'value': score, 'status': status}
    
    def _check_consistency(self, file_data):
        """Check data consistency"""
        metadata = file_data.get('metadata', '{}')
        file_type = file_data.get('file_type', '')
        content_type = file_data.get('content_type', '')
        
        # Check if file type matches content type
        if file_type != content_type:
            return {'value': 0.5, 'status': 'fair'}
        
        return {'value': 0.9, 'status': 'good'}
    
    def _check_uniqueness(self, text):
        """Basic uniqueness check"""
        words = text.split()
        unique_words = set(words)
        uniqueness_ratio = len(unique_words) / max(len(words), 1)
        
        status = 'good' if uniqueness_ratio > 0.7 else 'fair'
        return {'value': uniqueness_ratio, 'status': status}
    
    def _check_accuracy(self, file_data):
        """Basic accuracy check"""
        # Placeholder for accuracy checks
        # In real implementation, this would validate against known standards
        return {'value': 0.8, 'status': 'good'}