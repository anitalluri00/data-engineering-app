import os
import uuid
import hashlib
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional

class Helpers:
    @staticmethod
    def generate_unique_id(prefix: str = "") -> str:
        """Generate a unique ID with optional prefix"""
        unique_id = str(uuid.uuid4())
        return f"{prefix}_{unique_id}" if prefix else unique_id
    
    @staticmethod
    def calculate_file_hash(file_data: bytes) -> str:
        """Calculate MD5 hash of file data"""
        return hashlib.md5(file_data).hexdigest()
    
    @staticmethod
    def safe_json_serialize(obj: Any) -> str:
        """Safely serialize object to JSON string"""
        def default_serializer(o):
            if isinstance(o, (datetime,)):
                return o.isoformat()
            elif isinstance(o, (bytes,)):
                return o.decode('utf-8', errors='ignore')
            raise TypeError(f"Object of type {type(o)} is not JSON serializable")
        
        return json.dumps(obj, default=default_serializer, ensure_ascii=False)
    
    @staticmethod
    def parse_json_safe(json_str: str) -> Optional[Dict]:
        """Safely parse JSON string"""
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return None
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Format file size in human-readable format"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.2f} {size_names[i]}"
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Basic email validation"""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def chunk_list(lst: List, chunk_size: int) -> List[List]:
        """Split list into chunks of specified size"""
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    
    @staticmethod
    def get_file_extension(filename: str) -> str:
        """Get file extension in lowercase"""
        return os.path.splitext(filename)[1].lower()
    
    @staticmethod
    def is_supported_file_type(filename: str, supported_extensions: List[str]) -> bool:
        """Check if file type is supported"""
        ext = Helpers.get_file_extension(filename)
        return ext in supported_extensions
    
    @staticmethod
    def create_directory_if_not_exists(directory_path: str) -> bool:
        """Create directory if it doesn't exist"""
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except OSError as e:
            logging.error(f"Error creating directory {directory_path}: {e}")
            return False
    
    @staticmethod
    def clean_filename(filename: str) -> str:
        """Clean filename by removing invalid characters"""
        import re
        # Remove characters that are not allowed in filenames
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        return cleaned.strip('_. ')
    
    @staticmethod
    def get_timestamp_string() -> str:
        """Get current timestamp as string"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    @staticmethod
    def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp string to datetime object"""
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            return None
    
    @staticmethod
    def time_ago(dt: datetime) -> str:
        """Get human-readable time ago string"""
        now = datetime.now()
        diff = now - dt
        
        if diff < timedelta(minutes=1):
            return "just now"
        elif diff < timedelta(hours=1):
            minutes = int(diff.total_seconds() // 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif diff < timedelta(days=1):
            hours = int(diff.total_seconds() // 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif diff < timedelta(days=30):
            days = diff.days
            return f"{days} day{'s' if days != 1 else ''} ago"
        elif diff < timedelta(days=365):
            months = diff.days // 30
            return f"{months} month{'s' if months != 1 else ''} ago"
        else:
            years = diff.days // 365
            return f"{years} year{'s' if years != 1 else ''} ago"
    
    @staticmethod
    def retry_operation(operation, max_attempts: int = 3, delay: float = 1.0, 
                       exceptions: tuple = (Exception,)):
        """Retry an operation with exponential backoff"""
        import time
        
        for attempt in range(max_attempts):
            try:
                return operation()
            except exceptions as e:
                if attempt == max_attempts - 1:
                    raise e
                sleep_time = delay * (2 ** attempt)  # Exponential backoff
                logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {sleep_time}s")
                time.sleep(sleep_time)
    
    @staticmethod
    def validate_config(config: Dict, required_keys: List[str]) -> bool:
        """Validate configuration dictionary"""
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            logging.error(f"Missing required configuration keys: {missing_keys}")
            return False
        return True
    
    @staticmethod
    def sanitize_sql_value(value: Any) -> str:
        """Basic SQL value sanitization"""
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "1" if value else "0"
        else:
            # Escape single quotes for SQL
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"