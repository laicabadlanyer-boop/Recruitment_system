"""
Rate limiting utility for protecting endpoints from abuse.
Uses in-memory storage (for development) - consider Redis for production.
"""
from functools import wraps
from flask import request, jsonify, flash, redirect, url_for
from datetime import datetime, timedelta
from collections import defaultdict
import threading

# In-memory storage for rate limiting
_rate_limit_store = defaultdict(list)
_rate_limit_lock = threading.Lock()

def rate_limit(max_requests=5, window_seconds=300, per_ip=True, message="Too many requests. Please try again later."):
    """
    Rate limiting decorator.
    
    Args:
        max_requests: Maximum number of requests allowed
        window_seconds: Time window in seconds
        per_ip: If True, rate limit per IP address; if False, per endpoint
        message: Error message to display
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get identifier (IP address or endpoint)
            if per_ip:
                identifier = request.remote_addr or 'unknown'
            else:
                identifier = request.endpoint or 'unknown'
            
            key = f"{f.__name__}:{identifier}"
            now = datetime.now()
            
            with _rate_limit_lock:
                # Clean old entries
                _rate_limit_store[key] = [
                    timestamp for timestamp in _rate_limit_store[key]
                    if (now - timestamp).total_seconds() < window_seconds
                ]
                
                # Check if limit exceeded
                if len(_rate_limit_store[key]) >= max_requests:
                    if request.is_json or request.headers.get('Content-Type') == 'application/json':
                        return jsonify({'error': message}), 429
                    flash(message, 'error')
                    return redirect(request.referrer or url_for('index')), 429
                
                # Add current request
                _rate_limit_store[key].append(now)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


