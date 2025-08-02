"""
Stats manager for tracking operation statistics and handling recovery.
"""
import os
import json
import time
import asyncio
import datetime
from contextlib import asynccontextmanager
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import Channel, ChannelForbidden, InputPeerEmpty
import re
import logging

logger = logging.getLogger(__name__)

# Directory for storing operation stats
STATS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "stats")

# Create stats directory if it doesn't exist
if not os.path.exists(STATS_DIR):
    os.makedirs(STATS_DIR)

# Stats file paths
OPERATION_STATS_FILE = os.path.join(STATS_DIR, "operation_stats.json")
PERFORMANCE_METRICS_FILE = os.path.join(STATS_DIR, "performance_metrics.json")

# In-memory cache of operation stats
operation_stats = {}
performance_metrics = {}

# Load existing stats from files
def load_stats():
    """Load operation stats and performance metrics from files"""
    global operation_stats, performance_metrics
    
    # Load operation stats
    if os.path.exists(OPERATION_STATS_FILE):
        try:
            with open(OPERATION_STATS_FILE, 'r') as f:
                operation_stats = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading operation stats: {str(e)}")
            operation_stats = {}
    
    # Load performance metrics
    if os.path.exists(PERFORMANCE_METRICS_FILE):
        try:
            with open(PERFORMANCE_METRICS_FILE, 'r') as f:
                performance_metrics = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading performance metrics: {str(e)}")
            performance_metrics = {}

# Save stats to files
def save_stats():
    """Save operation stats and performance metrics to files"""
    # Save operation stats
    try:
        with open(OPERATION_STATS_FILE, 'w') as f:
            json.dump(operation_stats, f, indent=2)
    except IOError as e:
        print(f"Error saving operation stats: {str(e)}")
    
    # Save performance metrics
    try:
        with open(PERFORMANCE_METRICS_FILE, 'w') as f:
            json.dump(performance_metrics, f, indent=2)
    except IOError as e:
        print(f"Error saving performance metrics: {str(e)}")

# Load stats at module import
load_stats()

# Operation tracker context manager
@asynccontextmanager
async def OperationTracker(operation_type, operation_id=None):
    """
    Context manager for tracking operation statistics
    
    Args:
        operation_type (str): Type of operation (e.g., "message_sending", "user_adding")
        operation_id (str, optional): Unique ID for this operation. Generated if not provided.
    
    Yields:
        dict: Operation tracking information
    """
    # Generate operation ID if not provided
    if operation_id is None:
        operation_id = f"{operation_type}_{int(time.time())}"
    
    # Initialize operation stats if this is a new operation type
    if operation_type not in operation_stats:
        operation_stats[operation_type] = {
            "total_operations": 0,
            "total_time": 0,
            "total_items": 0,
            "success_count": 0,
            "failure_count": 0,
            "average_time_per_item": 0,
            "last_operation_timestamp": 0,
            "history": [],  # Limited history of recent operations
            "phase_timings": {}  # New: Track time spent in different phases
        }
    
    # Start tracking this operation
    start_time = time.time()
    current_stats = {
        "operation_id": operation_id,
        "start_time": start_time,
        "status": "running",
        "phase_timings": {},  # New: Track phase timings for this operation
        "current_phase": None  # New: Track current phase
    }
    
    try:
        # Yield control back to the calling function
        yield current_stats
        
        # Operation completed successfully
        current_stats["status"] = "completed"
        current_stats["end_time"] = time.time()
        current_stats["duration"] = current_stats["end_time"] - start_time
        
        # Update operation stats
        update_operation_stats(operation_type, current_stats, success=True)
        
    except Exception as e:
        # Operation failed
        current_stats["status"] = "failed"
        current_stats["end_time"] = time.time()
        current_stats["duration"] = current_stats["end_time"] - start_time
        current_stats["error"] = str(e)
        
        # Update operation stats
        update_operation_stats(operation_type, current_stats, success=False)
        
        # Re-raise the exception
        raise
    finally:
        # Always save stats
        save_stats()

def update_operation_stats(operation_type, current_stats, success=True, items_processed=0):
    """
    Update operation statistics based on the outcome of an operation
    
    Args:
        operation_type (str): Type of operation
        current_stats (dict): Current operation statistics
        success (bool): Whether the operation was successful
        items_processed (int): Number of items processed in this operation
    """
    # Update operation type stats
    stats = operation_stats[operation_type]
    stats["total_operations"] += 1
    stats["total_time"] += current_stats["duration"]
    
    if "items_processed" in current_stats:
        items_processed = current_stats["items_processed"]
    
    stats["total_items"] += items_processed
    
    if success:
        stats["success_count"] += 1
    else:
        stats["failure_count"] += 1
    
    # Calculate average time per item
    if stats["total_items"] > 0:
        stats["average_time_per_item"] = stats["total_time"] / stats["total_items"]
    
    # Update timestamp
    stats["last_operation_timestamp"] = time.time()
    
    # Add to history (keep last 10 operations)
    history_entry = {
        "operation_id": current_stats["operation_id"],
        "timestamp": current_stats["end_time"],
        "duration": current_stats["duration"],
        "success": success
    }
    
    if "error" in current_stats:
        history_entry["error"] = current_stats["error"]
    
    # Add phase timings if available
    if "phase_timings" in current_stats and current_stats["phase_timings"]:
        history_entry["phase_timings"] = current_stats["phase_timings"]
        
        # Update overall phase timing stats
        for phase, time_spent in current_stats["phase_timings"].items():
            if "phase_timings" not in stats:
                stats["phase_timings"] = {}
                
            if phase not in stats["phase_timings"]:
                stats["phase_timings"][phase] = {
                    "total_time": 0,
                    "count": 0,
                    "average_time": 0
                }
                
            stats["phase_timings"][phase]["total_time"] += time_spent
            stats["phase_timings"][phase]["count"] += 1
            stats["phase_timings"][phase]["average_time"] = (
                stats["phase_timings"][phase]["total_time"] / 
                stats["phase_timings"][phase]["count"]
            )
    
    stats["history"].append(history_entry)
    
    # Limit history size
    if len(stats["history"]) > 10:
        stats["history"] = stats["history"][-10:]
    
    # Update performance metrics
    update_performance_metrics(operation_type, current_stats, success, items_processed)

def update_performance_metrics(operation_type, current_stats, success, items_processed):
    """
    Update performance metrics for specific operation types
    
    Args:
        operation_type (str): Type of operation
        current_stats (dict): Current operation statistics
        success (bool): Whether the operation was successful
        items_processed (int): Number of items processed in this operation
    """
    # Initialize metrics for this operation type if not exists
    if operation_type not in performance_metrics:
        performance_metrics[operation_type] = {
            "hourly_rates": {},
            "daily_rates": {},
            "peak_rate": 0,
            "average_rate": 0,
            "recent_rates": [],
            "completion_time_variance": 0,  # New: Track variance in completion times
            "completion_times": []  # New: Track recent completion times
        }
    
    metrics = performance_metrics[operation_type]
    
    # Only update metrics if some items were processed
    if items_processed > 0 and current_stats["duration"] > 0:
        # Calculate rate for this operation
        rate = items_processed / current_stats["duration"]
        
        # Update recent rates (keep last 20)
        metrics["recent_rates"].append(rate)
        if len(metrics["recent_rates"]) > 20:
            metrics["recent_rates"] = metrics["recent_rates"][-20:]
        
        # Update average rate
        metrics["average_rate"] = sum(metrics["recent_rates"]) / len(metrics["recent_rates"])
        
        # Update peak rate if higher
        if rate > metrics["peak_rate"]:
            metrics["peak_rate"] = rate
        
        # New: Track completion time per item
        completion_time = current_stats["duration"] / items_processed
        
        # New: Update completion times list (keep last 20)
        if "completion_times" not in metrics:
            metrics["completion_times"] = []
        
        metrics["completion_times"].append(completion_time)
        if len(metrics["completion_times"]) > 20:
            metrics["completion_times"] = metrics["completion_times"][-20:]
        
        # New: Calculate variance in completion times
        if len(metrics["completion_times"]) > 1:
            mean_time = sum(metrics["completion_times"]) / len(metrics["completion_times"])
            variance = sum((t - mean_time) ** 2 for t in metrics["completion_times"]) / len(metrics["completion_times"])
            metrics["completion_time_variance"] = variance
        
        # Update hourly and daily rates
        hour_key = datetime.datetime.now().strftime("%Y-%m-%d-%H")
        day_key = datetime.datetime.now().strftime("%Y-%m-%d")
        
        if hour_key not in metrics["hourly_rates"]:
            metrics["hourly_rates"][hour_key] = {
                "items": 0,
                "time": 0
            }
        
        if day_key not in metrics["daily_rates"]:
            metrics["daily_rates"][day_key] = {
                "items": 0,
                "time": 0
            }
        
        # Update hourly metrics
        metrics["hourly_rates"][hour_key]["items"] += items_processed
        metrics["hourly_rates"][hour_key]["time"] += current_stats["duration"]
        
        # Update daily metrics
        metrics["daily_rates"][day_key]["items"] += items_processed
        metrics["daily_rates"][day_key]["time"] += current_stats["duration"]
        
        # Clean up old metrics (keep only last 24 hours and 30 days)
        now = datetime.datetime.now()
        
        # Clean hourly rates (keep 24 hours)
        hourly_keys = list(metrics["hourly_rates"].keys())
        for key in hourly_keys:
            try:
                key_dt = datetime.datetime.strptime(key, "%Y-%m-%d-%H")
                if (now - key_dt).total_seconds() > 86400:  # 24 hours
                    del metrics["hourly_rates"][key]
            except ValueError:
                # Invalid key format, remove it
                del metrics["hourly_rates"][key]
        
        # Clean daily rates (keep 30 days)
        daily_keys = list(metrics["daily_rates"].keys())
        for key in daily_keys:
            try:
                key_dt = datetime.datetime.strptime(key, "%Y-%m-%d")
                if (now - key_dt).days > 30:
                    del metrics["daily_rates"][key]
            except ValueError:
                # Invalid key format, remove it
                del metrics["daily_rates"][key]

async def calculate_eta(operation_type, item_count, completed_count=0, elapsed_time=0):
    """
    Calculate estimated time to completion based on historical performance
    
    Args:
        operation_type (str): Type of operation
        item_count (int): Total number of items to process
        completed_count (int): Number of items already processed
        elapsed_time (float): Time elapsed so far in seconds
    
    Returns:
        tuple: (eta_seconds, eta_formatted, confidence)
    """
    # Default values
    default_eta_seconds = (item_count - completed_count) * 1.5  # Assume 1.5 seconds per item as default
    confidence = "low"  # Default confidence level
    
    # If we have elapsed time for the current operation, use it for initial estimate
    if completed_count > 0 and elapsed_time > 0:
        current_rate = completed_count / elapsed_time
        initial_eta = (item_count - completed_count) / current_rate
    else:
        initial_eta = default_eta_seconds
    
    # Check if we have stats for this operation type
    if operation_type not in operation_stats:
        return default_eta_seconds, format_time_duration(default_eta_seconds), confidence
    
    # Get operation stats
    stats = operation_stats[operation_type]
    
    # If we have average time per item, use it
    if stats["average_time_per_item"] > 0:
        historical_eta = (item_count - completed_count) * stats["average_time_per_item"]
        confidence = "medium"
    else:
        # No valid average time, use default
        historical_eta = default_eta_seconds
    
    # Initialize final ETA to a weighted combination of current and historical
    if completed_count > 0 and elapsed_time > 0:
        # Weight the current performance more heavily
        eta_seconds = (initial_eta * 0.7) + (historical_eta * 0.3)
    else:
        eta_seconds = historical_eta
    
    # Check if we have more precise recent metrics
    if operation_type in performance_metrics:
        metrics = performance_metrics[operation_type]
        
        # If we have recent rates, use average of recent rates
        if metrics.get("recent_rates"):
            recent_avg_rate = metrics["average_rate"]
            if recent_avg_rate > 0:
                # Rate is items per second, so divide item count by rate
                recent_eta = (item_count - completed_count) / recent_avg_rate
                
                # Adjust confidence based on variance
                if "completion_time_variance" in metrics and metrics["completion_time_variance"] > 0:
                    variance = metrics["completion_time_variance"]
                    avg_time = 1 / recent_avg_rate  # Average time per item
                    
                    # Calculate coefficient of variation (lower is better)
                    cv = (variance ** 0.5) / avg_time
                    
                    if cv < 0.2:  # Low variance
                        confidence = "high"
                    elif cv < 0.5:  # Medium variance
                        confidence = "medium"
                    else:  # High variance
                        confidence = "low"
                else:
                    confidence = "medium"
                
                # If we have current operation data, blend with recent metrics
                if completed_count > 0 and elapsed_time > 0:
                    # Weighted average with more weight to current operation data
                    eta_seconds = (initial_eta * 0.5) + (recent_eta * 0.3) + (historical_eta * 0.2)
                else:
                    # More weight to recent metrics without current operation data
                    eta_seconds = (recent_eta * 0.7) + (historical_eta * 0.3)
    
    # Adjust based on the operation's historical phases if applicable
    if completed_count > 0 and "phase_timings" in stats and stats["phase_timings"]:
        # Try to identify which phase we're in based on completed percentage
        completion_percentage = (completed_count / item_count) * 100
        
        # Simple phase detection based on percentage ranges
        # Assuming operations might have different phases like "init", "processing", "finalization"
        current_phase = None
        if completion_percentage < 10:
            current_phase = "init"
        elif completion_percentage < 90:
            current_phase = "processing"
        else:
            current_phase = "finalization"
        
        # If we have timing data for the detected phase, adjust the ETA
        if current_phase in stats["phase_timings"]:
            phase_data = stats["phase_timings"][current_phase]
            if phase_data["count"] > 0:
                # Adjust ETA slightly based on historical phase timing
                phase_factor = 1.0  # Default: no adjustment
                
                # Different phases might have different processing speeds
                if current_phase == "init" and completion_percentage < 5:
                    # Initial phase might be slower than average
                    phase_factor = 1.2
                elif current_phase == "finalization" and completion_percentage > 95:
                    # Final phase might also be slower
                    phase_factor = 1.1
                
                # Apply phase adjustment factor
                eta_seconds *= phase_factor
    
    # Add a buffer based on confidence level
    if confidence == "low":
        eta_seconds *= 1.2  # Add 20% buffer for low confidence
    elif confidence == "medium":
        eta_seconds *= 1.1  # Add 10% buffer for medium confidence
    else:
        eta_seconds *= 1.05  # Add 5% buffer for high confidence
    
    # Format the ETA
    eta_formatted = format_time_duration(eta_seconds)
    
    return eta_seconds, eta_formatted, confidence

def record_operation_phase(stats, phase_name):
    """
    Record a phase transition in an operation
    
    Args:
        stats (dict): Operation statistics dictionary
        phase_name (str): Name of the new phase
    
    Returns:
        dict: Updated stats dictionary
    """
    current_time = time.time()
    
    # If there's a current phase, record its duration
    if stats.get("current_phase"):
        if "phase_start_time" in stats:
            phase_duration = current_time - stats["phase_start_time"]
            
            # Initialize phase_timings if needed
            if "phase_timings" not in stats:
                stats["phase_timings"] = {}
            
            # Record time spent in the previous phase
            prev_phase = stats["current_phase"]
            stats["phase_timings"][prev_phase] = phase_duration
    
    # Set the new phase
    stats["current_phase"] = phase_name
    stats["phase_start_time"] = current_time
    
    return stats

def format_time_duration(seconds):
    """
    Format a time duration in seconds to a human-readable string
    
    Args:
        seconds (float): Duration in seconds
    
    Returns:
        str: Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        minutes = (seconds % 3600) / 60
        return f"{int(hours)}h {int(minutes)}m"

def get_operation_status(operation_id):
    """
    Get the status of a specific operation
    
    Args:
        operation_id (str): Operation ID
    
    Returns:
        dict: Operation status or None if not found
    """
    # Check all operation types for this operation ID
    for operation_type, stats in operation_stats.items():
        for history_entry in stats["history"]:
            if history_entry["operation_id"] == operation_id:
                return history_entry
    
    return None

def get_performance_summary():
    """
    Get a summary of performance metrics for all operation types
    
    Returns:
        dict: Performance summary
    """
    summary = {}
    
    for operation_type, metrics in performance_metrics.items():
        summary[operation_type] = {
            "average_rate": metrics["average_rate"],
            "peak_rate": metrics["peak_rate"]
        }
        
        # Add variance data if available
        if "completion_time_variance" in metrics:
            summary[operation_type]["completion_time_variance"] = metrics["completion_time_variance"]
            
            # Calculate coefficient of variation if possible
            if metrics["average_rate"] > 0:
                avg_time = 1 / metrics["average_rate"]
                cv = (metrics["completion_time_variance"] ** 0.5) / avg_time
                summary[operation_type]["coefficient_of_variation"] = cv
        
        # Get most recent daily rate
        if metrics["daily_rates"]:
            latest_day = max(metrics["daily_rates"].keys())
            daily_data = metrics["daily_rates"][latest_day]
            
            if daily_data["time"] > 0:
                daily_rate = daily_data["items"] / daily_data["time"]
                summary[operation_type]["latest_daily_rate"] = daily_rate
                summary[operation_type]["latest_daily_items"] = daily_data["items"]
    
    return summary

# Initialize recovery tracking
async def track_recovery_point(operation_type, checkpoint_data):
    """
    Track a recovery checkpoint for an operation
    
    Args:
        operation_type (str): Type of operation
        checkpoint_data (dict): Checkpoint data to save
    """
    recovery_file = os.path.join(STATS_DIR, f"{operation_type}_recovery.json")
    
    try:
        with open(recovery_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except IOError as e:
        print(f"Error saving recovery checkpoint: {str(e)}")

async def get_recovery_point(operation_type):
    """
    Get the latest recovery checkpoint for an operation
    
    Args:
        operation_type (str): Type of operation
    
    Returns:
        dict: Checkpoint data or None if not found
    """
    recovery_file = os.path.join(STATS_DIR, f"{operation_type}_recovery.json")
    
    if not os.path.exists(recovery_file):
        return None
    
    try:
        with open(recovery_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error loading recovery checkpoint: {str(e)}")
        return None

async def clear_recovery_point(operation_type):
    """
    Clear the recovery checkpoint for an operation
    
    Args:
        operation_type (str): Type of operation
    """
    recovery_file = os.path.join(STATS_DIR, f"{operation_type}_recovery.json")
    
    if os.path.exists(recovery_file):
        try:
            os.remove(recovery_file)
        except IOError as e:
            print(f"Error clearing recovery checkpoint: {str(e)}")

class OperationTracker:
    """
    Track statistics for operations and provide ETA calculations
    """
    
    def __init__(self, operation_type, total_items=None, session_phone=None):
        """
        Initialize an operation tracker
        
        Args:
            operation_type: Type of operation being tracked
            total_items: Total number of items to process (if known)
            session_phone: Phone number of the session being used
        """
        self.operation_type = operation_type
        self.total_items = total_items
        self.processed_items = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.completion_times = []
        self.session_phone = session_phone
        self.errors = 0
        self.paused = False
        self.pause_start = None
        self.total_pause_time = 0
        self.status_updates = []
        self.last_eta_calculation = 0
        self.last_eta = None
        
    def update(self, items_processed=1, error=False, status_message=None):
        """
        Update progress on the operation
        
        Args:
            items_processed: Number of items processed in this update
            error: Whether an error occurred
            status_message: Optional status message to record
        
        Returns:
            dict: Progress information including ETA if available
        """
        current_time = time.time()
        
        # Record item completion time
        if not error:
            self.processed_items += items_processed
            
            # Only record completion time if not an error
            if items_processed > 0:
                time_per_item = (current_time - self.last_update_time) / items_processed
                self.completion_times.append(time_per_item)
                
                # Keep only the last 50 completion times for more accurate recent estimates
                if len(self.completion_times) > 50:
                    self.completion_times = self.completion_times[-50:]
        else:
            self.errors += 1
        
        # Record status message if provided
        if status_message:
            self.status_updates.append({
                'timestamp': current_time,
                'message': status_message,
                'processed_items': self.processed_items,
                'errors': self.errors
            })
            
            # Keep only the last 20 status updates
            if len(self.status_updates) > 20:
                self.status_updates = self.status_updates[-20:]
        
        # Update last update time
        self.last_update_time = current_time
        
        # Only calculate ETA every 2 seconds to avoid unnecessary calculations
        if current_time - self.last_eta_calculation >= 2:
            eta_info = self.calculate_eta()
            self.last_eta = eta_info.get('estimated_completion_time')
            self.last_eta_calculation = current_time
        else:
            eta_info = {
                'estimated_completion_time': self.last_eta,
                'estimated_time_remaining': self.last_eta - current_time if self.last_eta else None,
                'percent_complete': (self.processed_items / self.total_items * 100) if self.total_items else None
            }
        
        return {
            'operation_type': self.operation_type,
            'processed_items': self.processed_items,
            'total_items': self.total_items,
            'errors': self.errors,
            'elapsed_time': current_time - self.start_time - self.total_pause_time,
            'progress': eta_info
        }
    
    def calculate_eta(self):
        """
        Calculate estimated time of completion based on progress
        
        Returns:
            dict: ETA information including estimated completion time,
                 time remaining, and percent complete
        """
        if not self.total_items or self.processed_items == 0:
            return {
                'estimated_completion_time': None,
                'estimated_time_remaining': None,
                'percent_complete': None
            }
        
        current_time = time.time()
        elapsed_time = current_time - self.start_time - self.total_pause_time
        
        # Calculate average time per item
        if self.completion_times:
            # Use weighted average giving more weight to recent times
            weights = [i/len(self.completion_times) for i in range(1, len(self.completion_times)+1)]
            weighted_times = [t * w for t, w in zip(self.completion_times, weights)]
            avg_time_per_item = sum(weighted_times) / sum(weights)
        else:
            # Fallback to simple average if no completion times recorded
            avg_time_per_item = elapsed_time / self.processed_items
        
        # Calculate remaining items and time
        remaining_items = self.total_items - self.processed_items
        estimated_remaining_time = remaining_items * avg_time_per_item
        
        # Factor in recent slowdowns or speedups
        if len(self.completion_times) >= 10:
            recent_times = self.completion_times[-10:]
            older_times = self.completion_times[:-10]
            
            if older_times:
                recent_avg = sum(recent_times) / len(recent_times)
                older_avg = sum(older_times) / len(older_times)
                
                # Adjust for trend (speeding up or slowing down)
                trend_factor = recent_avg / older_avg if older_avg > 0 else 1
                
                # Limit the impact of extreme values
                trend_factor = max(0.5, min(trend_factor, 1.5))
                
                estimated_remaining_time *= trend_factor
        
        # Calculate estimated completion time
        estimated_completion_time = current_time + estimated_remaining_time
        
        # Calculate percent complete
        percent_complete = (self.processed_items / self.total_items) * 100
        
        return {
            'estimated_completion_time': estimated_completion_time,
            'estimated_time_remaining': estimated_remaining_time,
            'percent_complete': percent_complete,
            'time_per_item': avg_time_per_item,
            'items_per_minute': 60 / avg_time_per_item if avg_time_per_item > 0 else 0
        }
    
    def pause(self):
        """Pause the operation timer"""
        if not self.paused:
            self.paused = True
            self.pause_start = time.time()
    
    def resume(self):
        """Resume the operation timer"""
        if self.paused and self.pause_start:
            self.paused = False
            pause_duration = time.time() - self.pause_start
            self.total_pause_time += pause_duration
            self.pause_start = None
    
    def get_formatted_progress(self, include_eta=True):
        """
        Get a formatted progress message
        
        Args:
            include_eta: Whether to include ETA information
        
        Returns:
            str: Formatted progress message
        """
        if not self.total_items:
            return f"Processed {self.processed_items} items"
        
        # Calculate progress percentage
        percent = (self.processed_items / self.total_items) * 100
        
        # Create progress bar
        bar_length = 20
        filled_length = int(bar_length * self.processed_items // self.total_items)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        progress_msg = f"[{bar}] {self.processed_items}/{self.total_items} ({percent:.1f}%)"
        
        if include_eta and self.total_items and self.processed_items > 0:
            eta_info = self.calculate_eta()
            remaining = eta_info['estimated_time_remaining']
            
            if remaining:
                # Format remaining time
                if remaining < 60:
                    eta_str = f"{int(remaining)} seconds"
                elif remaining < 3600:
                    eta_str = f"{int(remaining / 60)} minutes"
                else:
                    eta_str = f"{remaining / 3600:.1f} hours"
                
                progress_msg += f" - ETA: {eta_str} remaining"
        
        return progress_msg
    
    def get_summary(self):
        """
        Get a summary of the operation
        
        Returns:
            dict: Operation summary
        """
        end_time = time.time()
        duration = end_time - self.start_time - self.total_pause_time
        
        return {
            'operation_type': self.operation_type,
            'total_items': self.total_items,
            'processed_items': self.processed_items,
            'errors': self.errors,
            'duration_seconds': duration,
            'items_per_second': self.processed_items / duration if duration > 0 else 0,
            'items_per_minute': (self.processed_items / duration) * 60 if duration > 0 else 0,
            'success_rate': ((self.processed_items - self.errors) / self.processed_items * 100) 
                            if self.processed_items > 0 else 0
        }

def format_time_remaining(seconds):
    """
    Format seconds into a human-readable time string
    
    Args:
        seconds: Number of seconds
    
    Returns:
        str: Formatted time string
    """
    if seconds is None:
        return "unknown"
    
    if seconds < 60:
        return f"{int(seconds)} seconds"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''}"
    else:
        days = int(seconds / 86400)
        hours = int((seconds % 86400) / 3600)
        return f"{days} day{'s' if days != 1 else ''} {hours} hour{'s' if hours != 1 else ''}"

def get_progress_bar(current, total, width=20):
    """
    Generate a text-based progress bar
    
    Args:
        current: Current progress value
        total: Total value
        width: Width of the progress bar in characters
    
    Returns:
        str: ASCII progress bar
    """
    if total <= 0:
        percent = 0
    else:
        percent = current / total
    
    filled_width = int(width * percent)
    bar = '█' * filled_width + '░' * (width - filled_width)
    percent_str = f"{percent*100:.1f}%"
    
    return f"[{bar}] {current}/{total} ({percent_str})"

async def analyze_user_groups(session_string, api_id, api_hash):
    """
    Analyze groups created by a specific user session
    
    Args:
        session_string: The user's session string
        api_id: Telegram API ID
        api_hash: Telegram API Hash
    
    Returns:
        dict: Statistics about the user's groups
    """
    try:
        # Create client with the session string
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.connect()
        
        if not await client.is_user_authorized():
            return {
                "success": False,
                "error": "Session not authorized",
                "data": None
            }
        
        # Get user info
        me = await client.get_me()
        username = f"@{me.username}" if me.username else f"{me.first_name} {me.last_name if me.last_name else ''}"
        user_id = me.id
        
        # Initialize statistics
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
        stats = {
            "user_id": user_id,
            "username": username,
            "total_groups": 0,
            "today_groups": 0,
            "yesterday_groups": 0,
            "this_week_groups": 0,
            "groups_by_date": {},
            "available_groups": 0,
            "creator_of": [],
            "admin_in": []
        }
        
        # Get all dialogs (chats and channels)
        dialogs = []
        
        try:
            # Fetch all dialogs in chunks
            offset_date = None
            offset_id = 0
            offset_peer = InputPeerEmpty()
            chunk_size = 100
            
            while True:
                result = await client(GetDialogsRequest(
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_peer=offset_peer,
                    limit=chunk_size,
                    hash=0
                ))
                
                if not result.dialogs:
                    break
                    
                for dialog in result.dialogs:
                    # Make sure we have access to the entity directly
                    try:
                        # Skip dialogs without entities
                        if not hasattr(dialog, "entity") or not dialog.entity:
                            # Try to get entity from the messages
                            peer_id = dialog.peer
                            for msg in result.messages:
                                if msg.peer_id == peer_id:
                                    try:
                                        entity = await client.get_entity(peer_id)
                                        if entity and isinstance(entity, (Channel, ChannelForbidden)):
                                            dialogs.append(dialog)
                                            break
                                    except:
                                        pass
                            continue
                        
                        if isinstance(dialog.entity, (Channel, ChannelForbidden)):
                            dialogs.append(dialog)
                    except Exception as e:
                        logger.warning(f"Error processing dialog: {str(e)}")
                        continue
                
                if len(result.dialogs) < chunk_size:
                    break
                
                offset_date = result.messages[-1].date
                offset_id = result.messages[-1].id
                offset_peer = result.dialogs[-1].peer
        except Exception as e:
            logger.error(f"Error fetching dialogs: {str(e)}")
            # Even if fetching dialogs fails, continue with the groups we could get
        
        # Get channels where user is creator or admin
        channels = []
        for dialog in dialogs:
            try:
                if hasattr(dialog, "entity") and dialog.entity and isinstance(dialog.entity, (Channel, ChannelForbidden)):
                    if hasattr(dialog.entity, "megagroup") and dialog.entity.megagroup:
                        channels.append(dialog.entity)
                elif hasattr(dialog, "peer"):
                    # Try to get the entity from the peer
                    try:
                        entity = await client.get_entity(dialog.peer)
                        if entity and isinstance(entity, (Channel, ChannelForbidden)):
                            if hasattr(entity, "megagroup") and entity.megagroup:
                                channels.append(entity)
                    except Exception as e:
                        logger.warning(f"Could not get entity for peer: {str(e)}")
            except Exception as e:
                logger.warning(f"Error processing channel: {str(e)}")
                continue
        
        # Extract additional channel info
        for channel in channels:
            try:
                # Skip channels that aren't supergroups
                if not hasattr(channel, "megagroup") or not channel.megagroup:
                    continue
                
                # Get full channel info
                if hasattr(channel, "id"):
                    try:
                        full_channel = await client.get_entity(channel.id)
                        stats["total_groups"] += 1
                        
                        # Check if creator
                        is_creator = full_channel.creator if hasattr(full_channel, "creator") else False
                        
                        # Get creation date if possible
                        # Extract from group name using regex pattern like "Group_1_1_1596283946"
                        # where the last number is a timestamp
                        creation_timestamp = None
                        group_name = full_channel.title
                        timestamp_match = re.search(r'[A-Za-z]+_\d+_\d+_(\d+)', group_name)
                        
                        if timestamp_match:
                            creation_timestamp = int(timestamp_match.group(1))
                        else:
                            # If not in name, use first message date as approximate
                            try:
                                messages = await client.get_messages(full_channel.id, limit=1, reverse=True)
                                if messages and messages[0]:
                                    creation_timestamp = int(messages[0].date.timestamp())
                            except:
                                # If can't determine, use None
                                pass
                        
                        if creation_timestamp:
                            creation_date = datetime.datetime.fromtimestamp(creation_timestamp).strftime('%Y-%m-%d')
                            
                            # Update stats by date
                            if creation_date not in stats["groups_by_date"]:
                                stats["groups_by_date"][creation_date] = 0
                            
                            stats["groups_by_date"][creation_date] += 1
                            
                            # Check if created today
                            if creation_date == today:
                                stats["today_groups"] += 1
                            
                            # Check if created yesterday
                            if creation_date == yesterday:
                                stats["yesterday_groups"] += 1
                            
                            # Check if created this week
                            week_start = (datetime.datetime.now() - datetime.timedelta(days=datetime.datetime.now().weekday())).strftime('%Y-%m-%d')
                            if creation_date >= week_start:
                                stats["this_week_groups"] += 1
                        
                        # Add to creator/admin list
                        group_info = {
                            "id": full_channel.id,
                            "title": full_channel.title,
                            "username": f"@{full_channel.username}" if hasattr(full_channel, "username") and full_channel.username else None,
                            "members_count": full_channel.participants_count if hasattr(full_channel, "participants_count") else None,
                            "date": creation_date if creation_timestamp else None
                        }
                        
                        if is_creator:
                            stats["creator_of"].append(group_info)
                        else:
                            stats["admin_in"].append(group_info)
                    except Exception as e:
                        logger.warning(f"Error getting full channel info for {channel.id}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.warning(f"Error getting info for channel {channel.id if hasattr(channel, 'id') else 'unknown'}: {str(e)}")
                continue
        
        # Calculate how many groups can be created today
        from config import DAILY_GROUP_LIMIT
        stats["available_groups"] = DAILY_GROUP_LIMIT - stats["today_groups"]
        
        # Sort groups by date
        stats["creator_of"] = sorted(stats["creator_of"], 
                                    key=lambda x: x["date"] if x["date"] else "9999-99-99", 
                                    reverse=True)
        
        await client.disconnect()
        
        return {
            "success": True,
            "data": stats,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Error analyzing user groups: {str(e)}")
        # Try to disconnect client if it exists
        try:
            if 'client' in locals() and client:
                await client.disconnect()
        except:
            pass
            
        return {
            "success": False,
            "error": str(e),
            "data": None
        }

async def get_group_creation_summary(chat_id):
    """
    Get a summary of all group creation activity for a specific chat
    
    Args:
        chat_id: The chat ID to get summary for
    
    Returns:
        dict: Summary data
    """
    from modules.multi_group_creator import USER_SESSIONS, CREATED_GROUPS
    
    if chat_id not in USER_SESSIONS or chat_id not in CREATED_GROUPS:
        return {
            "success": False,
            "error": "No group creation data found for this chat",
            "data": None
        }
    
    # Get session data
    session_data = USER_SESSIONS[chat_id]
    groups_data = CREATED_GROUPS[chat_id]
    
    # Prepare summary
    summary = {
        "total_sessions": session_data["max_users"] if "max_users" in session_data else 0,
        "total_groups": len(groups_data),
        "groups_by_session": {},
        "groups_by_date": {}
    }
    
    # Process group data
    for group in groups_data:
        session_idx = group["session_index"]
        created_at = group["created_at"].split()[0]  # Just get the date part
        
        # Count by session
        if session_idx not in summary["groups_by_session"]:
            summary["groups_by_session"][session_idx] = 0
        summary["groups_by_session"][session_idx] += 1
        
        # Count by date
        if created_at not in summary["groups_by_date"]:
            summary["groups_by_date"][created_at] = 0
        summary["groups_by_date"][created_at] += 1
    
    return {
        "success": True,
        "data": summary,
        "error": None
    }