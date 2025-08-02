"""
Load balancer utility for managing Telegram sessions.
Handles detection of overloaded sessions and redirecting operations to alternate accounts.
"""

import asyncio
import time
import logging
import os
import json
import random
from typing import Dict, List, Tuple, Optional, Callable, Any
from telethon import TelegramClient

# Import utility modules
from utils.floodwait import handle_flood_wait, is_flood_error
from utils.stats_manager import OperationTracker

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger('LoadBalancer')

# Session health status constants
STATUS_HEALTHY = "healthy"
STATUS_DEGRADED = "degraded"
STATUS_OVERLOADED = "overloaded"
STATUS_DISABLED = "disabled"
STATUS_COOLING = "cooling"  # New status for cooling down overloaded sessions

# Constants for rate limiting and health monitoring
MAX_ERRORS_THRESHOLD = 5  # Maximum number of errors in window before marking session unhealthy
ERROR_WINDOW_SECONDS = 300  # 5 minutes window for counting errors
MAX_FLOODWAIT_THRESHOLD = 60  # Maximum floodwait in seconds before reducing load
HIGH_LOAD_OPERATIONS_PER_MINUTE = 30  # Operations per minute that indicate high load
COOL_DOWN_PERIOD = 600  # 10 minutes cool down for overloaded sessions
HEALTH_CHECK_INTERVAL = 60  # Check session health every minute

class LoadBalancer:
    """
    Load balancer for managing multiple Telegram sessions
    """
    
    def __init__(self, config_path=None):
        """Initialize the load balancer"""
        self.active_clients = {}  # phone -> client
        self.session_metrics = {}  # phone -> metrics
        self.last_health_check = 0
        
        self.sessions = {}
        self.session_health = {}
        self.operation_counters = {}
        self.error_counters = {}
        self.last_used = {}
        self.flood_wait_times = {}
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "config",
            "load_balancer.json"
        )
        
        # Ensure config directory exists
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        
        # Load config if exists
        self.config = self._load_config()
        
        # Initialize session metrics
        self.session_metrics = {}
        
        # Initialize baseline response times for operations
        self.baseline_response_times = {
            "default": 1.0,
            "message": 1.2,
            "join": 2.0,
            "admin": 1.5,
            "scrape": 2.5
        }
        
        # Enhanced overload detection thresholds
        self.overload_thresholds = {
            "error_rate": 0.2,  # 20% error rate
            "flood_wait_frequency": 5,  # 5 flood waits in time window
            "flood_wait_time_window": 3600,  # 1 hour
            "consecutive_errors": 3,  # 3 consecutive errors
            "response_time_factor": 2.0,  # Response time 2x baseline
            "operation_rate_acceleration": 2.0,  # Rapid doubling of operation rate
            "timeout_frequency": 3,  # 3 timeouts in window
            "combined_warning_threshold": 1.0,  # Combined warning score threshold
            "pattern_recognition_weight": 0.7,  # Weight for pattern-based detection
        }
        
        # New metrics for sophisticated detection
        self.session_pattern_history = {}  # Phone -> list of historical patterns
        
        # Recovery settings
        self.recovery = {
            "cool_down_period": 300,  # 5 minutes
            "max_retries": 3,  # Maximum retries for overloaded sessions
            "backoff_factor": 1.5,  # Exponential backoff factor
        }
        
        # Track active operations per session
        self.active_operations = {}
        self.session_cooldowns = {}  # phone -> cooldown end time
        
        # Enhanced adaptive cooldown parameters
        self.cooldown_parameters = {
            "base_cooldown": 300,  # 5 minutes base cooldown
            "max_cooldown": 3600,  # 1 hour maximum cooldown
            "escalation_factor": 1.5,  # Increase by 50% each time
            "de_escalation_factor": 0.8,  # Decrease by 20% for successful recovery
            "success_recovery_threshold": 10,  # 10 successful operations to reduce cooldown
            "repeated_issue_threshold": 3,  # 3 repeated issues to increase escalation
            "cooldown_jitter": 0.2,  # 20% random jitter to prevent thundering herd
            "severity_weights": {
                "flood_wait": 1.2,  # Flood wait errors weighted more heavily
                "timeout": 1.1,  # Timeout errors weighted heavily
                "network": 1.0,  # Network errors standard weight
                "auth": 1.3,  # Auth errors weighted most heavily
                "other": 0.8,  # Other errors less weight
            }
        }
        
        # Adaptive cooldown tracking
        self.session_cooldown_history = {}  # Phone -> list of past cooldowns
        
        # Load history for prediction
        self.load_history = []  # (timestamp, {"global_load": value, "session_loads": {phone: value}})
        self.global_load_threshold = 100  # Global load threshold for high load detection
        
        # Session priorities for user behavior-based prioritization
        self.session_priorities = {}  # phone -> priority level
        self.default_priority = 5     # Default priority level (1-10 scale)
        self.min_priority = 1         # Minimum priority level
        self.max_priority = 10        # Maximum priority level
        self.priority_throttle_map = {  # Maps priority levels to throttle factors
            1: 5.0,    # Lowest priority - highest throttling
            2: 4.0,
            3: 3.0,
            4: 2.0,
            5: 1.5,    # Default - moderate throttling
            6: 1.2,
            7: 1.1,
            8: 1.05,
            9: 1.01,
            10: 1.0    # Highest priority - no throttling
        }
        
        # Behavioral pattern analysis
        self.behavior_patterns = {}   # phone -> pattern data
        self.pattern_window = 86400   # Analyze patterns over 24 hours
        self.last_priority_update = 0 # Last time priorities were updated
    
    def _load_config(self):
        """
        Load load balancer configuration from file.
        
        Returns:
            dict: Load balancer configuration
        """
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                logger.info(f"Loaded load balancer config from {self.config_path}")
                return config
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error loading load balancer config: {str(e)}")
        
        # Default config
        return {
            "strategy": "round_robin",  # round_robin, least_used, weighted_random
            "enable_health_checks": True,
            "health_check_interval": 300,  # 5 minutes
            "auto_disable_threshold": 0.5,  # Disable sessions with 50% error rate
            "max_retries": 3,  # Maximum retries for failed operations
            "cooldown_period": 60,  # 1 minute cooldown after flood wait
            "weights": {}  # Session weights for weighted selection
        }
    
    def _save_config(self):
        """Save load balancer configuration to file."""
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Saved load balancer config to {self.config_path}")
        except IOError as e:
            logger.error(f"Error saving load balancer config: {str(e)}")
    
    async def register_client(self, phone: str, client: TelegramClient, 
                             purpose: str = "general", is_primary: bool = True):
        """
        Register a client with the load balancer
        
        Args:
            phone: Phone number of the session
            client: TelegramClient instance
            purpose: Purpose category for this client (e.g., "message", "admin", "scraping")
            is_primary: Whether this is a primary session or backup
        """
        # Store client reference
        self.active_clients[phone] = client
        
        # Initialize metrics if needed
        if phone not in self.session_metrics:
            self.session_metrics[phone] = {
                "operations": 0,
                "errors": 0,
                "flood_waits": 0,
                "total_flood_wait_time": 0,
                "last_flood_wait": 0,
                "response_times": {},
                "error_timestamps": [],
                "health_score": 1.0,  # 1.0 = perfectly healthy
                "consecutive_errors": 0,
                "overload_history": []
            }
        
        # Register in appropriate pool
        if purpose not in self.sessions:
            self.sessions[purpose] = {"primary": [], "backup": []}
        
        pool_type = "primary" if is_primary else "backup"
        if phone not in self.sessions[purpose][pool_type]:
            self.sessions[purpose][pool_type].append(phone)
            
        logger.info(f"Registered client {phone} for {purpose} ({pool_type})")
    
    async def calculate_adaptive_cooldown(self, phone: str, error_type: str = "general") -> int:
        """
        Calculate an adaptive cooldown period based on session history and error patterns
        
        Args:
            phone: Phone number of the session
            error_type: Type of error that triggered the cooldown
            
        Returns:
            int: Recommended cooldown period in seconds
        """
        if phone not in self.session_metrics:
            # Default cooldown for unknown session
            return self.cooldown_parameters["base_cooldown"]
            
        metrics = self.session_metrics[phone]
        base_cooldown = self.cooldown_parameters["base_cooldown"]
        
        # Check if we have cooldown history for this session
        phone_history = self.session_cooldown_history.get(phone, [])
        
        # Initialize cooldown with base value
        cooldown_period = base_cooldown
        
        # 1. Escalation based on repeated issues
        if len(phone_history) >= 2:
            # Count recent cooldowns (last 6 hours)
            recent_cooldowns = [c for c in phone_history 
                              if time.time() - c["timestamp"] <= 21600]  # 6 hours
            
            if len(recent_cooldowns) >= self.cooldown_parameters["repeated_issue_threshold"]:
                # Progressive escalation for repeated issues
                escalation_count = min(len(recent_cooldowns), 5)  # Cap at 5x escalation
                cooldown_period *= pow(self.cooldown_parameters["escalation_factor"], escalation_count - 1)
                logger.debug(f"Session {phone} cooldown escalated {escalation_count}x due to repeated issues")
        
        # 2. Error type weighting
        error_weight = self.cooldown_parameters["severity_weights"].get(
            error_type, self.cooldown_parameters["severity_weights"]["other"]
        )
        cooldown_period *= error_weight
        
        # 3. FloodWait-based adjustment
        if "last_flood_wait" in metrics and metrics["last_flood_wait"] > 0:
            # If we have flood wait history, adjust cooldown based on it
            recent_flood_waits = [fw for ts, fw in self.flood_wait_times.get(phone, [])
                                if time.time() - ts <= 3600]  # Last hour
            
            if recent_flood_waits:
                # Base cooldown on the maximum recent flood wait time (but at least our base)
                max_flood_wait = max(recent_flood_waits)
                flood_cooldown = max(max_flood_wait * 2, base_cooldown)
                
                # Use the higher of our calculated cooldown or the flood-based one
                cooldown_period = max(cooldown_period, flood_cooldown)
                logger.debug(f"Session {phone} cooldown adjusted for flood wait: {max_flood_wait}s → {flood_cooldown}s")
        
        # 4. Pattern-based adjustment for recurring issues
        if "overload_history" in metrics and len(metrics["overload_history"]) >= 3:
            timestamps = [entry["timestamp"] for entry in metrics["overload_history"]]
            if len(timestamps) >= 3:
                # Calculate average interval between overloads
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
                avg_interval = sum(intervals) / len(intervals)
                
                # Calculate consistency (lower deviation = more consistent pattern)
                deviation = sum(abs(i - avg_interval) for i in intervals) / len(intervals)
                consistency = 1 - min(deviation / avg_interval, 0.5)  # 0.5-1.0 range
                
                if consistency > 0.7:  # Highly consistent pattern
                    # Set cooldown to just beyond the average interval to break the cycle
                    pattern_cooldown = avg_interval * 1.2
                    cooldown_period = max(cooldown_period, pattern_cooldown)
                    logger.debug(f"Session {phone} cooldown adjusted for pattern: {pattern_cooldown:.0f}s (consistency: {consistency:.2f})")
        
        # 5. Recovery-based de-escalation
        successful_ops_since_last_issue = metrics.get("successful_ops_since_last_issue", 0)
        if successful_ops_since_last_issue >= self.cooldown_parameters["success_recovery_threshold"] and phone_history:
            # De-escalate cooldown if we've had successful operations since last issue
            de_escalation_factor = self.cooldown_parameters["de_escalation_factor"]
            
            # Find the last cooldown period
            if phone_history:
                last_cooldown = phone_history[-1]["duration"]
                
                # De-escalate from the last cooldown period
                cooldown_period = max(base_cooldown, last_cooldown * de_escalation_factor)
                logger.debug(f"Session {phone} cooldown de-escalated due to successful recovery: {last_cooldown}s → {cooldown_period:.0f}s")
        
        # 6. Add jitter to prevent thundering herd
        jitter_factor = random.uniform(
            1 - self.cooldown_parameters["cooldown_jitter"],
            1 + self.cooldown_parameters["cooldown_jitter"]
        )
        cooldown_period *= jitter_factor
        
        # Ensure within min/max bounds
        cooldown_period = min(
            max(cooldown_period, self.cooldown_parameters["base_cooldown"]),
            self.cooldown_parameters["max_cooldown"]
        )
        
        # Record this cooldown in history
        if phone not in self.session_cooldown_history:
            self.session_cooldown_history[phone] = []
            
        self.session_cooldown_history[phone].append({
            "timestamp": time.time(),
            "duration": cooldown_period,
            "error_type": error_type
        })
        
        # Trim history if needed
        if len(self.session_cooldown_history[phone]) > 10:
            self.session_cooldown_history[phone] = self.session_cooldown_history[phone][-10:]
        
        return int(cooldown_period)
        
    async def cool_down_session(self, phone: str, error_type: str = "general") -> int:
        """
        Put a session in cooldown mode with adaptive duration
        
        Args:
            phone: Phone number of the session
            error_type: Type of error that triggered the cooldown
            
        Returns:
            int: Actual cooldown duration in seconds
        """
        # Calculate adaptive cooldown duration
        duration = await self.calculate_adaptive_cooldown(phone, error_type)
        
        # Record cooldown
        self.session_cooldowns[phone] = time.time() + duration
        
        # Set status to cooling
        if phone in self.session_metrics:
            self.session_metrics[phone]["status"] = STATUS_COOLING
            self.session_metrics[phone]["cooling_start"] = time.time()
            self.session_metrics[phone]["cooling_duration"] = duration
            self.session_metrics[phone]["cooling_end"] = time.time() + duration
            self.session_metrics[phone]["cooling_reason"] = error_type
            
            # Reset error counters for a fresh start after cooldown
            self.session_metrics[phone]["consecutive_errors"] = 0
            self.session_metrics[phone]["successful_ops_since_last_issue"] = 0
        
        logger.info(f"Session {phone} in adaptive cooldown for {duration}s due to {error_type} error")
        return duration
    
    async def is_in_cooldown(self, phone: str) -> bool:
        """
        Check if a session is currently in cooldown
        
        Args:
            phone: Phone number to check
            
        Returns:
            bool: True if session is in cooldown, False otherwise
        """
        if phone not in self.session_cooldowns:
            return False
            
        cooldown_until = self.session_cooldowns[phone]
        if time.time() > cooldown_until:
            # Cooldown period expired
            del self.session_cooldowns[phone]
            return False
            
        return True
    
    async def get_best_client(self, operation_type: str = "default") -> TelegramClient:
        """
        Get the best client for a specific operation type based on load and status
        
        Args:
            operation_type: Type of operation to perform (default: "default")
            
        Returns:
            Client: The best Telegram client to use
        """
        available_clients = []
        
        for phone, client in self.active_clients.items():
            # Skip clients in cooldown
            if await self.is_in_cooldown(phone):
                continue
                
            # Skip overloaded clients
            if await self.detect_session_overload(phone):
                # If overloaded, put in cooldown automatically
                await self.cool_down_session(phone)
                continue
                
            available_clients.append((phone, client))
            
        if not available_clients:
            # If all clients are in cooldown or overloaded, pick the one with 
            # the least time left in cooldown
            least_cooldown_time = float('inf')
            least_cooldown_phone = None
            
            for phone in self.session_cooldowns:
                remaining = self.session_cooldowns[phone] - time.time()
                if remaining < least_cooldown_time:
                    least_cooldown_time = remaining
                    least_cooldown_phone = phone
                    
            if least_cooldown_phone:
                logger.warning(f"All sessions overloaded, using {least_cooldown_phone} with {least_cooldown_time:.1f}s cooldown remaining")
                return self.active_clients[least_cooldown_phone]
            else:
                # Fallback to the first client if no cooldowns are active
                first_phone = list(self.active_clients.keys())[0]
                logger.warning(f"All sessions overloaded, using first available: {first_phone}")
                return self.active_clients[first_phone]
                
        # Calculate load score for each available client
        client_scores = []
        for phone, client in available_clients:
            score = await self.calculate_client_score(phone, operation_type)
            client_scores.append((score, phone, client))
            
        # Sort by score (lower is better)
        client_scores.sort()
        
        # Return the client with the best score
        _, best_phone, best_client = client_scores[0]
        return best_client
    
    async def calculate_client_score(self, phone: str, operation_type: str) -> float:
        """
        Calculate a score for a client based on its current load and performance
        Lower score is better
        
        Args:
            phone: Phone number of the client
            operation_type: Type of operation to be performed
            
        Returns:
            float: Score representing client suitability (lower is better)
        """
        if phone not in self.session_metrics:
            # New or reset client gets a good score
            return 0.5
            
        metrics = self.session_metrics[phone]
        current_time = time.time()
        score = 0.0
        
        # Recent operations factor (more operations = higher score)
        recent_ops = len([t for t in metrics.get("operation_timestamps", []) 
                         if current_time - t <= 300])  # Last 5 minutes
        score += min(recent_ops / 50, 1.0)  # Cap at 1.0
        
        # Recent errors factor
        recent_errors = len([t for t in metrics.get("error_timestamps", []) 
                           if current_time - t <= 300])
        score += recent_errors * 0.5  # Each recent error adds 0.5 to score
        
        # Response time factor for this operation type
        if operation_type in metrics.get("response_times", {}):
            times = metrics["response_times"][operation_type]
            recent_times = [t for ts, t in times if current_time - ts <= 300]
            if recent_times:
                avg_time = sum(recent_times) / len(recent_times)
                # Fix: safely get baseline with fallback to default
                baseline = self.baseline_response_times.get(
                    operation_type, 
                    self.baseline_response_times.get("default", 1.0)
                )
                time_factor = avg_time / baseline
                score += min(time_factor - 1, 1.0)  # Only add if slower than baseline
        
        # Flood wait factor
        flood_waits = len([fw for ts, fw in self.flood_wait_times.get(phone, [])
                         if current_time - ts <= 900])  # Last 15 minutes
        score += flood_waits * 0.3  # Each flood wait adds 0.3 to score
        
        # Add small random factor to avoid always picking the same client
        score += random.uniform(0, 0.1)
        
        return score
    
    async def get_best_client(self, purpose: str) -> Tuple[Optional[str], Optional[TelegramClient]]:
        """
        Get the best available client for a specific purpose, prioritizing by session priority
        
        Args:
            purpose: Purpose category for the client
        
        Returns:
            Tuple of (phone, client) or (None, None) if no suitable client
        """
        # First do a health check if needed
        current_time = time.time()
        if current_time - self.last_health_check > HEALTH_CHECK_INTERVAL:
            await self.check_all_sessions_health()
            self.last_health_check = current_time
            
        # Also update priorities if needed
        if current_time - self.last_priority_update > 600:  # Every 10 minutes
            await self.update_session_priorities()
            self.last_priority_update = current_time
        
        # If purpose doesn't exist, try to use general
        if purpose not in self.sessions:
            if "general" not in self.sessions:
                return None, None
            purpose = "general"
        
        # Collect all available sessions for this purpose
        available_sessions = []
        
        # Check primary pool first
        for phone in self.sessions[purpose]["primary"]:
            if phone in self.active_clients and not await self.is_in_cooldown(phone):
                # Get session priority (default to 5 if not set)
                priority = self.session_priorities.get(phone, self.default_priority)
                health_score = self.session_metrics.get(phone, {}).get("health_score", 1.0)
                
                # Add to available sessions with combined score
                # Higher priority and health score = better
                combined_score = priority * health_score
                available_sessions.append((combined_score, phone))
        
        # Then check backup pool if needed
        if not available_sessions:
            for phone in self.sessions[purpose]["backup"]:
                if phone in self.active_clients and not await self.is_in_cooldown(phone):
                    priority = self.session_priorities.get(phone, self.default_priority)
                    health_score = self.session_metrics.get(phone, {}).get("health_score", 1.0)
                    
                    # Backup sessions get a slightly lower base score
                    combined_score = priority * health_score * 0.9
                    available_sessions.append((combined_score, phone))
        
        # Sort by combined score (higher is better)
        available_sessions.sort(reverse=True)
        
        # If we found available sessions, return the best one
        if available_sessions:
            best_score, best_phone = available_sessions[0]
            return best_phone, self.active_clients.get(best_phone)
        
        # As a last resort, try an unhealthy client that's not cooling
        for pool_type in ["primary", "backup"]:
            for phone in self.sessions[purpose][pool_type]:
                if phone in self.active_clients and phone in self.session_metrics:
                    if self.session_metrics[phone]["status"] != STATUS_COOLING:
                        return phone, self.active_clients.get(phone)
        
        # No suitable client found
        return None, None
    
    async def update_session_priorities(self):
        """
        Update session priorities based on behavior patterns
        """
        current_time = time.time()
        
        for phone, metrics in self.session_metrics.items():
            if phone not in self.session_priorities:
                # Initialize with default priority
                self.session_priorities[phone] = self.default_priority
            
            # Skip sessions without enough data
            if "operations" not in metrics or metrics["operations"] < 10:
                continue
                
            # Calculate behavior score factors
            
            # 1. Error rate factor (lower is better)
            error_rate = len(metrics.get("error_timestamps", [])) / max(metrics["operations"], 1)
            error_factor = 0
            if error_rate < 0.01:  # Very low error rate
                error_factor = 2
            elif error_rate < 0.05:  # Low error rate
                error_factor = 1
            elif error_rate > 0.20:  # High error rate
                error_factor = -1
            elif error_rate > 0.40:  # Very high error rate
                error_factor = -2
                
            # 2. Flood wait factor (lower is better)
            flood_waits = len(self.flood_wait_times.get(phone, []))
            flood_factor = 0
            if flood_waits == 0:  # No flood waits
                flood_factor = 1
            elif flood_waits > 3:  # Multiple flood waits
                flood_factor = -1
            elif flood_waits > 5:  # Many flood waits
                flood_factor = -2
                
            # 3. Operation frequency factor (penalize spammy behavior)
            recent_ops = len([t for t in metrics.get("operation_timestamps", []) 
                             if current_time - t <= 600])  # Last 10 minutes
            frequency_factor = 0
            if recent_ops > 100:  # Very high frequency
                frequency_factor = -2
            elif recent_ops > 50:  # High frequency
                frequency_factor = -1
            elif recent_ops < 10:  # Low frequency
                frequency_factor = 1
                
            # 4. User type bonus
            user_type = metrics.get("user_type", "regular")
            type_factor = 0
            if user_type == "premium":
                type_factor = 1
            elif user_type == "admin":
                type_factor = 2
            
            # 5. Critical operation bonus
            critical_factor = 1 if metrics.get("critical_operation", False) else 0
            
            # 6. Consistent good behavior bonus
            consistent_good = metrics.get("consistent_good_behavior", False)
            consistency_factor = 1 if consistent_good else 0
            
            # Calculate adjustment (positive = higher priority)
            adjustment = error_factor + flood_factor + frequency_factor + type_factor + critical_factor + consistency_factor
            
            # Apply adjustment with dampening (max ±2 change at once)
            capped_adjustment = max(min(adjustment, 2), -2)
            new_priority = self.session_priorities[phone] + capped_adjustment
            
            # Ensure within bounds
            new_priority = max(min(new_priority, self.max_priority), self.min_priority)
            
            # Only update if changed
            if new_priority != self.session_priorities[phone]:
                logger.debug(f"Updated priority for {phone}: {self.session_priorities[phone]} → {new_priority}")
                self.session_priorities[phone] = new_priority
                
            # Track consistency for future bonuses
            if error_factor > 0 and flood_factor >= 0:
                # Increment consecutive good behavior counter
                metrics["good_behavior_streak"] = metrics.get("good_behavior_streak", 0) + 1
                
                # Mark as consistent good behavior after a streak
                if metrics["good_behavior_streak"] >= 5:  # 5 consecutive good updates
                    metrics["consistent_good_behavior"] = True
            else:
                # Reset streak on bad behavior
                metrics["good_behavior_streak"] = 0
                metrics["consistent_good_behavior"] = False
    
    async def apply_priority_throttling(self, phone: str) -> float:
        """
        Get throttling factor based on session priority
        
        Args:
            phone: Session phone number
            
        Returns:
            float: Throttling factor (1.0 = no throttling, higher = more throttling)
        """
        # Get priority level (default to 5)
        priority = self.session_priorities.get(phone, self.default_priority)
        
        # Get throttle factor from mapping
        throttle_factor = self.priority_throttle_map.get(priority, 1.5)  # Default moderate throttling
        
        return throttle_factor
    
    async def record_operation(self, phone: str, success: bool = True, 
                              error: Optional[Exception] = None, 
                              floodwait_seconds: int = 0,
                              operation_type: str = "general"):
        """
        Record an operation result for a session
        
        Args:
            phone: Phone number of the session
            success: Whether the operation was successful
            error: Exception that occurred (if any)
            floodwait_seconds: FloodWait duration in seconds (if applicable)
            operation_type: Type of operation performed
        """
        if phone not in self.session_metrics:
            self.session_metrics[phone] = {}
        
        current_time = time.time()
        
        # Initialize operation timestamps list if needed
        if "operation_timestamps" not in self.session_metrics[phone]:
            self.session_metrics[phone]["operation_timestamps"] = []
            
        # Record operation timestamp
        self.session_metrics[phone]["operation_timestamps"].append(current_time)
        
        # Keep only timestamps within the window
        self.session_metrics[phone]["operation_timestamps"] = [
            t for t in self.session_metrics[phone]["operation_timestamps"] 
            if current_time - t <= ERROR_WINDOW_SECONDS
        ]
        
        # Update operation count
        self.session_metrics[phone]["operations"] = len(self.session_metrics[phone]["operation_timestamps"])
        
        # Record operation by type
        if "operations_by_type" not in self.session_metrics[phone]:
            self.session_metrics[phone]["operations_by_type"] = {}
            
        if operation_type not in self.session_metrics[phone]["operations_by_type"]:
            self.session_metrics[phone]["operations_by_type"][operation_type] = []
            
        self.session_metrics[phone]["operations_by_type"][operation_type].append(current_time)
        
        # If operation failed, record error
        if not success and error:
            if "error_timestamps" not in self.session_metrics[phone]:
                self.session_metrics[phone]["error_timestamps"] = []
                
            self.session_metrics[phone]["error_timestamps"].append(time.time())
            self.session_metrics[phone]["consecutive_errors"] = self.session_metrics[phone].get("consecutive_errors", 0) + 1
            
            # Keep only timestamps within the window
            self.session_metrics[phone]["error_timestamps"] = [
                t for t in self.session_metrics[phone]["error_timestamps"] 
                if current_time - t <= ERROR_WINDOW_SECONDS
            ]
        else:
            # Reset consecutive errors on success
            self.session_metrics[phone]["consecutive_errors"] = 0
            
            # Track successful operations for priority bonuses
            self.session_metrics[phone]["successful_ops_since_last_issue"] = self.session_metrics[phone].get("successful_ops_since_last_issue", 0) + 1
        
        # If floodwait occurred, record it
        if floodwait_seconds > 0:
            if "flood_waits" not in self.flood_wait_times:
                self.flood_wait_times[phone] = []
                
            self.flood_wait_times[phone].append((time.time(), floodwait_seconds))
            
            # Keep only flood waits within the time window
            time_window = self.overload_thresholds["flood_wait_time_window"]
            cutoff = time.time() - time_window
            
            self.flood_wait_times[phone] = [
                (ts, wt) for ts, wt in self.flood_wait_times[phone]
                if ts >= cutoff
            ]
            
            # Reset successful operations counter on flood wait
            self.session_metrics[phone]["successful_ops_since_last_issue"] = 0
        
        # Check this session's health
        await self.check_session_health(phone)
        
    async def execute_with_priority(self, purpose: str, func: Callable, *args, 
                                  operation_type: str = "general",
                                  priority_override: Optional[int] = None,
                                  **kwargs) -> Tuple[Any, bool, str]:
        """
        Execute a function with priority-based throttling
        
        Args:
            purpose: Purpose category for the client
            func: Async function to execute (should take client as first argument)
            operation_type: Type of operation being performed
            priority_override: Optional priority level override
            *args, **kwargs: Arguments to pass to the function
        
        Returns:
            Tuple of (result, success, phone)
            - result: The result of the function or the exception if it failed
            - success: Whether the operation succeeded
            - phone: Phone number of the session used
        """
        phone, client = await self.get_best_client(purpose)
        
        if not client:
            return None, False, ""
        
        # Get priority throttling factor
        if priority_override is not None:
            # Use override if provided
            priority = max(min(priority_override, self.max_priority), self.min_priority)
            throttle_factor = self.priority_throttle_map.get(priority, 1.5)
        else:
            # Use session's calculated priority
            throttle_factor = await self.apply_priority_throttling(phone)
        
        # Apply throttling if needed
        if throttle_factor > 1.0:
            # Calculate delay based on throttle factor
            delay = (throttle_factor - 1.0) * random.uniform(0.5, 1.5)
            if delay > 0.1:  # Only delay if significant
                logger.debug(f"Priority throttling for {phone}: {delay:.2f}s delay (factor: {throttle_factor:.2f})")
                await asyncio.sleep(delay)
        
        floodwait_seconds = 0
        success = True
        error = None
        result = None
        
        try:
            # Execute the function with the client
            result = await func(client, *args, **kwargs)
            return result, True, phone
            
        except Exception as e:
            success = False
            error = e
            
            # Check if it's a flood wait error
            error_str = str(e).lower()
            if "floodwait" in error_str or "flood wait" in error_str:
                # Extract the wait time
                import re
                wait_match = re.search(r"(\d+)", error_str)
                if wait_match:
                    floodwait_seconds = int(wait_match.group(1))
            
            return e, False, phone
            
        finally:
            # Record this operation
            await self.record_operation(
                phone=phone,
                success=success,
                error=error,
                floodwait_seconds=floodwait_seconds,
                operation_type=operation_type
            )
            
    async def register_user_behavior(self, phone: str, behavior_type: str, value: Any = None):
        """
        Register a specific user behavior for priority calculation
        
        Args:
            phone: Session phone number
            behavior_type: Type of behavior (e.g., 'spam', 'efficient', 'abusive', 'helpful')
            value: Optional value associated with the behavior
        """
        if phone not in self.behavior_patterns:
            self.behavior_patterns[phone] = {}
            
        current_time = time.time()
        
        # Initialize behavior type if needed
        if behavior_type not in self.behavior_patterns[phone]:
            self.behavior_patterns[phone][behavior_type] = []
            
        # Add behavior instance
        self.behavior_patterns[phone][behavior_type].append({
            "timestamp": current_time,
            "value": value
        })
        
        # Trim old behaviors outside window
        self.behavior_patterns[phone][behavior_type] = [
            b for b in self.behavior_patterns[phone][behavior_type]
            if current_time - b["timestamp"] <= self.pattern_window
        ]
        
        # Calculate immediate priority adjustments for certain behaviors
        adjustment = 0
        
        if behavior_type == "spam":
            # Immediate penalty for spam behavior
            adjustment = -2
        elif behavior_type == "abusive":
            # Severe penalty for abusive behavior
            adjustment = -3
        elif behavior_type == "helpful":
            # Reward for helpful behavior
            adjustment = 1
            
        # Apply immediate adjustment if needed
        if adjustment != 0 and phone in self.session_priorities:
            new_priority = max(min(self.session_priorities[phone] + adjustment, 
                                  self.max_priority), self.min_priority)
            
            if new_priority != self.session_priorities[phone]:
                logger.info(f"Immediate priority adjustment for {phone} ({behavior_type}): {self.session_priorities[phone]} → {new_priority}")
                self.session_priorities[phone] = new_priority
    
    async def detect_session_overload(self, phone: str) -> bool:
        """
        Enhanced detection of session overload using multiple indicators and pattern recognition
        
        Args:
            phone: Phone number of the session to check
            
        Returns:
            bool: True if session is overloaded, False otherwise
        """
        if phone not in self.session_metrics:
            return False
            
        metrics = self.session_metrics[phone]
        current_time = time.time()
        
        # Calculate various overload indicators
        
        # 1. Error rate calculation
        recent_errors = len([t for t in metrics.get("error_timestamps", []) 
                          if current_time - t <= ERROR_WINDOW_SECONDS])
        recent_ops = max(len([t for t in metrics.get("operation_timestamps", []) 
                          if current_time - t <= ERROR_WINDOW_SECONDS]), 1)
        error_rate = recent_errors / recent_ops
        
        # 2. Flood wait frequency
        flood_waits = len([fw for ts, fw in self.flood_wait_times.get(phone, [])
                         if current_time - ts <= self.overload_thresholds["flood_wait_time_window"]])
        
        # 3. Response time degradation
        response_time_degradation = 0
        for op_type, times in metrics.get("response_times", {}).items():
            recent_times = [t for ts, t in times if current_time - ts <= 1800]  # Last 30 minutes
            if recent_times and op_type in self.baseline_response_times:
                avg_time = sum(recent_times) / len(recent_times)
                baseline = self.baseline_response_times[op_type]
                if avg_time > baseline * self.overload_thresholds["response_time_factor"]:
                    response_time_degradation += 1
        
        # 4. Operation rate acceleration
        if "operation_rate_history" in metrics:
            # Calculate current rate (ops per minute)
            current_rate = len([t for t in metrics.get("operation_timestamps", [])
                              if current_time - t <= 60]) # Last minute
            
            # Get previous rate
            prev_rates = metrics["operation_rate_history"]
            if prev_rates and current_rate > prev_rates[-1] * self.overload_thresholds["operation_rate_acceleration"]:
                # Sudden acceleration in operation rate detected
                logger.warning(f"Session {phone} shows operation rate acceleration: {prev_rates[-1]} -> {current_rate} ops/min")
                
        # 5. Pattern-based detection
        pattern_score = 0
        if "overload_history" in metrics and len(metrics["overload_history"]) >= 2:
            # Calculate time since last overload
            last_overload_time = metrics["overload_history"][-1]["timestamp"]
            time_since_last = current_time - last_overload_time
            
            # Analyze previous overload intervals
            intervals = []
            for i in range(1, len(metrics["overload_history"])):
                curr = metrics["overload_history"][i]["timestamp"]
                prev = metrics["overload_history"][i-1]["timestamp"]
                intervals.append(curr - prev)
                
            if intervals:
                avg_interval = sum(intervals) / len(intervals)
                
                # If we're approaching the average interval between overloads,
                # increase pattern score
                if time_since_last > avg_interval * 0.7:
                    pattern_score = self.overload_thresholds["pattern_recognition_weight"]
                    logger.debug(f"Session {phone} pattern-based warning: {time_since_last:.1f}s since last overload, avg interval: {avg_interval:.1f}s")
        
        # Combine indicators into a single score
        overload_score = 0
        
        # Add error rate component
        if error_rate >= self.overload_thresholds["error_rate"]:
            overload_score += error_rate / self.overload_thresholds["error_rate"]
            
        # Add flood wait component
        if flood_waits >= self.overload_thresholds["flood_wait_frequency"]:
            overload_score += flood_waits / self.overload_thresholds["flood_wait_frequency"]
            
        # Add consecutive errors component
        if metrics.get("consecutive_errors", 0) >= self.overload_thresholds["consecutive_errors"]:
            overload_score += metrics["consecutive_errors"] / self.overload_thresholds["consecutive_errors"]
            
        # Add response time degradation
        overload_score += response_time_degradation * 0.2  # Each degraded operation type adds 0.2
        
        # Add pattern-based component
        overload_score += pattern_score
        
        # Update metrics
        metrics["current_overload_score"] = overload_score
        
        # Record operation rate for future comparison
        if "operation_rate_history" not in metrics:
            metrics["operation_rate_history"] = []
            
        current_rate = len([t for t in metrics.get("operation_timestamps", [])
                          if current_time - t <= 60]) # Last minute
        metrics["operation_rate_history"].append(current_rate)
        
        # Keep only recent history
        if len(metrics["operation_rate_history"]) > 10:
            metrics["operation_rate_history"] = metrics["operation_rate_history"][-10:]
        
        # Check if overloaded
        is_overloaded = overload_score >= self.overload_thresholds["combined_warning_threshold"]
        
        # If overloaded, record in history
        if is_overloaded:
            if "overload_history" not in metrics:
                metrics["overload_history"] = []
                
            metrics["overload_history"].append({
                "timestamp": current_time,
                "score": overload_score,
                "error_rate": error_rate,
                "flood_waits": flood_waits,
                "consecutive_errors": metrics.get("consecutive_errors", 0)
            })
            
            # Update session status
            metrics["status"] = STATUS_OVERLOADED
            
            # Keep only recent history
            if len(metrics["overload_history"]) > 20:
                metrics["overload_history"] = metrics["overload_history"][-20:]
                
            logger.warning(f"Session {phone} marked as overloaded (score: {overload_score:.2f})")
            
        return is_overloaded
        
    async def implement_session_rotation(self, purpose: str, current_phone: Optional[str] = None):
        """
        Implements intelligent session rotation strategy
        
        Args:
            purpose: Purpose category for the client
            current_phone: Current phone number being used (optional)
            
        Returns:
            Tuple of (phone, client) for the rotated session
        """
        # If no current phone, just get the best client
        if not current_phone:
            return await self.get_best_client(purpose)
            
        # Get all available sessions for this purpose
        available_phones = []
        
        # First check same pool as current session
        current_pool = None
        for pool_type in ["primary", "backup"]:
            if current_phone in self.sessions.get(purpose, {}).get(pool_type, []):
                current_pool = pool_type
                break
                
        if not current_pool:
            # If current phone not found in any pool, default to primary
            current_pool = "primary"
            
        # Get available phones from the same pool
        for phone in self.sessions.get(purpose, {}).get(current_pool, []):
            if phone != current_phone and phone in self.active_clients and not await self.is_in_cooldown(phone):
                available_phones.append(phone)
                
        # If no available phones in same pool, check other pool
        if not available_phones:
            other_pool = "backup" if current_pool == "primary" else "primary"
            for phone in self.sessions.get(purpose, {}).get(other_pool, []):
                if phone != current_phone and phone in self.active_clients and not await self.is_in_cooldown(phone):
                    available_phones.append(phone)
                    
        # If still no available phones, we have to reuse the current one
        if not available_phones:
            logger.warning(f"No alternative sessions available for {purpose}, reusing {current_phone}")
            return current_phone, self.active_clients.get(current_phone)
            
        # Calculate load scores for available phones
        phone_scores = []
        for phone in available_phones:
            if phone in self.session_metrics:
                # Calculate score (lower is better)
                score = 0
                
                # Factor 1: Health score
                health_score = self.session_metrics[phone].get("health_score", 1.0)
                score -= health_score  # Better health reduces score
                
                # Factor 2: Priority
                priority = self.session_priorities.get(phone, self.default_priority)
                score -= priority / 10  # Higher priority reduces score
                
                # Factor 3: Recent operations
                recent_ops = len([t for t in self.session_metrics[phone].get("operation_timestamps", [])
                               if time.time() - t <= 300])  # Last 5 minutes
                score += recent_ops / 100  # More operations increases score
                
                # Factor 4: Small random factor for load distribution
                score += random.uniform(0, 0.1)
                
                # Store score
                phone_scores.append((score, phone))
            else:
                # New session gets a neutral score
                phone_scores.append((0, phone))
                
        # Sort by score (lower is better)
        phone_scores.sort()
        
        # Get best phone
        best_phone = phone_scores[0][1]
        
        logger.info(f"Rotating session from {current_phone} to {best_phone} for {purpose}")
        return best_phone, self.active_clients.get(best_phone)
        
    async def force_session_rotation(self, purpose: str, avoid_phone: str):
        """
        Forces rotation to a new session, avoiding a specific one that has issues
        
        Args:
            purpose: Purpose category
            avoid_phone: Phone number to avoid using
            
        Returns:
            Tuple of (phone, client) for the new session
        """
        # Put the problematic session in cooldown
        if avoid_phone in self.session_metrics:
            await self.cool_down_session(avoid_phone, "forced_rotation")
            
        # Get a new session through rotation, avoiding the problematic one
        return await self.implement_session_rotation(purpose, avoid_phone)