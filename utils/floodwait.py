"""
FloodWait Handler
Handles Telegram's rate limiting (FloodWait errors) gracefully with adaptive retry logic
"""

import asyncio
import logging
import random
from telethon.errors import FloodWaitError, ServerError, TimedOutError

logger = logging.getLogger(__name__)

async def safe_execute(func, *args, max_attempts=3, exponential_backoff=True, **kwargs):
    """
    Safely execute a function that might raise FloodWaitError with adaptive retry logic
    
    Args:
        func: The async function to execute
        *args: Positional arguments to pass to func
        max_attempts: Maximum number of retry attempts
        exponential_backoff: Whether to use exponential backoff for non-floodwait errors
        **kwargs: Keyword arguments to pass to func
    
    Returns:
        The result of the function call, or raises the last error after all attempts
    """
    attempt = 0
    last_exception = None
    
    while attempt < max_attempts:
        try:
            return await func(*args, **kwargs)
        
        except FloodWaitError as e:
            wait_time = e.seconds
            attempt += 1
            last_exception = e
            
            logger.warning(f"FloodWaitError: Need to wait {wait_time} seconds. Attempt {attempt}/{max_attempts}")
            
            if attempt < max_attempts:
                # Always respect Telegram's wait time for FloodWaitError
                logger.info(f"Waiting for {wait_time} seconds before retrying...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Maximum retry attempts ({max_attempts}) reached. Giving up.")
                raise
        
        except (ServerError, TimedOutError) as e:
            attempt += 1
            last_exception = e
            
            logger.warning(f"{e.__class__.__name__}: {str(e)}. Attempt {attempt}/{max_attempts}")
            
            if attempt < max_attempts:
                # Use exponential backoff with jitter for server errors
                if exponential_backoff:
                    wait_time = min(30, (2 ** attempt) + random.uniform(0, 1))
                else:
                    wait_time = 5
                
                logger.info(f"Waiting for {wait_time:.1f} seconds before retrying...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Maximum retry attempts ({max_attempts}) reached. Giving up.")
                raise
        
        except Exception as e:
            logger.error(f"Error executing function: {str(e)}")
            raise
    
    if last_exception:
        raise last_exception
    
    return None

async def with_cooldown(func, cooldown_seconds=60, *args, **kwargs):
    """
    Execute a function and then enforce a cooldown period
    
    Args:
        func: The async function to execute
        cooldown_seconds: The cooldown period in seconds
        *args: Positional arguments to pass to func
        **kwargs: Keyword arguments to pass to func
    
    Returns:
        The result of the function call
    """
    try:
        result = await func(*args, **kwargs)
        logger.info(f"Operation completed. Cooling down for {cooldown_seconds} seconds...")
        await asyncio.sleep(cooldown_seconds)
        return result
    except Exception as e:
        logger.error(f"Error during operation with cooldown: {str(e)}")
        # Still enforce cooldown even after error to prevent rate limiting
        await asyncio.sleep(cooldown_seconds)
        raise

class RateLimitManager:
    """
    Manages rate limiting across multiple sessions
    """
    def __init__(self):
        self.session_timestamps = {}
        self.operation_counts = {}
    
    async def execute_with_rate_limit(self, session_id, operation_type, func, *args, 
                                      min_interval=30, max_per_day=None, **kwargs):
        """
        Execute a function with rate limiting based on session and operation type
        
        Args:
            session_id: Identifier for the session
            operation_type: Type of operation (e.g., 'create_group', 'send_message')
            func: The async function to execute
            min_interval: Minimum interval between operations in seconds
            max_per_day: Maximum operations per day
            *args: Positional arguments to pass to func
            **kwargs: Keyword arguments to pass to func
        
        Returns:
            The result of the function call
        """
        # Check if we need to enforce rate limiting
        operation_key = f"{session_id}_{operation_type}"
        current_time = asyncio.get_event_loop().time()
        
        # Check daily limit
        if max_per_day:
            day_key = f"{operation_key}_{int(current_time / 86400)}"
            if day_key in self.operation_counts and self.operation_counts[day_key] >= max_per_day:
                raise Exception(f"Daily limit of {max_per_day} {operation_type} operations reached for session {session_id}")
            
            self.operation_counts[day_key] = self.operation_counts.get(day_key, 0) + 1
        
        # Check interval
        if operation_key in self.session_timestamps:
            last_time = self.session_timestamps[operation_key]
            elapsed = current_time - last_time
            
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                logger.info(f"Rate limiting: Waiting {wait_time:.1f}s for {operation_type} on session {session_id}")
                await asyncio.sleep(wait_time)
        
        # Execute the function
        result = await func(*args, **kwargs)
        
        # Update timestamp
        self.session_timestamps[operation_key] = asyncio.get_event_loop().time()
        
        return result

# Global rate limit manager
rate_limit_manager = RateLimitManager()