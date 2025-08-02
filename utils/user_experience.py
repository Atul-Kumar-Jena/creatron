"""
User Experience Utilities for Telegram userbot.
Provides progress indicators, ETA calculations, and conversation handlers.
"""

import asyncio
import time
import logging
import sys
import math
from typing import Dict, List, Callable, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from telethon import Button
import traceback

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger('UserExperience')

# Constants for progress display
DEFAULT_PROGRESS_BAR_LENGTH = 20
PROGRESS_CHARS = {
    "bar_start": "[",
    "bar_end": "]",
    "bar_filled": "█",
    "bar_empty": "░",
}

# Mapping for operation time estimates (operation_type -> average_seconds_per_item)
DEFAULT_OPERATION_TIMES = {
    "message_send": 0.5,
    "message_forward": 0.7,
    "user_add": 1.2,
    "group_creation": 2.0,
    "participant_scrape": 0.1,
    "media_upload": 3.0,
    "default": 1.0
}

class ETACalculator:
    """
    ETA calculator for estimating remaining time in operations
    """
    
    def __init__(self, total_items: int, operation_type: str = "default", 
                 custom_time_per_item: float = None):
        """
        Initialize ETA calculator
        
        Args:
            total_items: Total number of items to process
            operation_type: Type of operation (for predefined time estimates)
            custom_time_per_item: Custom time per item (in seconds)
        """
        self.total_items = total_items
        self.completed_items = 0
        self.operation_type = operation_type
        
        # Use custom time if provided, otherwise use default for operation type
        self.time_per_item = custom_time_per_item or DEFAULT_OPERATION_TIMES.get(
            operation_type, DEFAULT_OPERATION_TIMES["default"]
        )
        
        # Track progress
        self.start_time = None
        self.current_eta = None
        self.last_update_time = 0
        self.completion_times = []  # Track actual item completion times
        self.adaptive_time_per_item = self.time_per_item
        
        # Throttle update frequency (avoid updating too frequently)
        self.min_update_interval = 1.0  # seconds
    
    def start(self):
        """Start the operation and initialize timing"""
        self.start_time = time.time()
        self.last_update_time = self.start_time
        return self
    
    def update(self, completed_items: int) -> Dict[str, Any]:
        """
        Update progress and recalculate ETA
        
        Args:
            completed_items: Number of items completed so far
            
        Returns:
            Dictionary with updated progress information
        """
        current_time = time.time()
        
        # Throttle updates
        if (current_time - self.last_update_time < self.min_update_interval and 
            completed_items < self.total_items):
            # Return the last calculated values if we're updating too frequently
            if self.current_eta:
                return self.current_eta
        
        self.last_update_time = current_time
        
        # Track newly completed items
        new_items = completed_items - self.completed_items
        if new_items > 0 and self.completed_items > 0:
            # Calculate time per item for these new completions
            time_elapsed = current_time - self.last_update_time
            time_per_new_item = time_elapsed / new_items
            
            # Add to our completion times (keep last 10)
            self.completion_times.append(time_per_new_item)
            if len(self.completion_times) > 10:
                self.completion_times = self.completion_times[-10:]
            
            # Recalculate adaptive time per item (moving average)
            self.adaptive_time_per_item = sum(self.completion_times) / len(self.completion_times)
        
        self.completed_items = completed_items
        
        # Calculate progress percentage
        progress_pct = (self.completed_items / self.total_items) * 100 if self.total_items > 0 else 0
        
        # Calculate elapsed time
        elapsed_seconds = current_time - self.start_time
        
        # Calculate ETA
        remaining_items = self.total_items - self.completed_items
        remaining_seconds = remaining_items * self.adaptive_time_per_item
        
        # Calculate estimated total time
        total_time = elapsed_seconds + remaining_seconds
        
        # Format times as strings
        elapsed_str = self._format_time(elapsed_seconds)
        remaining_str = self._format_time(remaining_seconds)
        total_str = self._format_time(total_time)
        
        # Calculate processing rate
        if elapsed_seconds > 0:
            items_per_second = self.completed_items / elapsed_seconds
            items_per_minute = items_per_second * 60
        else:
            items_per_second = 0
            items_per_minute = 0
        
        # Prepare result
        result = {
            "progress_pct": progress_pct,
            "completed": self.completed_items,
            "total": self.total_items,
            "elapsed_seconds": elapsed_seconds,
            "elapsed_str": elapsed_str,
            "remaining_seconds": remaining_seconds,
            "remaining_str": remaining_str,
            "total_time_seconds": total_time,
            "total_time_str": total_str,
            "items_per_second": items_per_second,
            "items_per_minute": items_per_minute,
            "time_per_item": self.adaptive_time_per_item
        }
        
        self.current_eta = result
        return result
    
    def _format_time(self, seconds: float) -> str:
        """
        Format seconds as a human-readable string
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string (e.g. "2h 30m 45s")
        """
        if math.isnan(seconds) or seconds < 0:
            return "unknown"
            
        if seconds < 1:
            return "<1s"
            
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

class ProgressBar:
    """
    Text-based progress bar for console and message updates
    """
    
    def __init__(self, total: int, operation_type: str = "default", 
                 width: int = DEFAULT_PROGRESS_BAR_LENGTH,
                 style: Dict[str, str] = None):
        """
        Initialize progress bar
        
        Args:
            total: Total number of items
            operation_type: Type of operation (for ETA calculation)
            width: Width of the progress bar in characters
            style: Custom style characters for the progress bar
        """
        self.total = total
        self.width = width
        self.eta = ETACalculator(total, operation_type).start()
        
        # Use custom style if provided, otherwise use default
        self.style = style or PROGRESS_CHARS
    
    def update(self, current: int, extra_info: str = None) -> str:
        """
        Update the progress bar and return formatted string
        
        Args:
            current: Current progress value
            extra_info: Additional information to show after the progress bar
            
        Returns:
            Formatted progress bar string
        """
        # Get ETA information
        eta_info = self.eta.update(current)
        
        # Calculate percentage and number of filled blocks
        percentage = min(100, eta_info["progress_pct"])
        filled_length = int(self.width * current // self.total)
        filled_length = min(filled_length, self.width)  # Ensure we don't exceed width
        
        # Create the bar
        bar = (
            self.style["bar_start"] +
            self.style["bar_filled"] * filled_length +
            self.style["bar_empty"] * (self.width - filled_length) +
            self.style["bar_end"]
        )
        
        # Format the basic progress string
        progress_str = f"{bar} {percentage:.1f}% ({current}/{self.total})"
        
        # Add timing information
        timing_str = f"Elapsed: {eta_info['elapsed_str']} | Remaining: {eta_info['remaining_str']}"
        
        # Combine all parts
        result = progress_str + "\n" + timing_str
        
        # Add extra info if provided
        if extra_info:
            result += f"\n{extra_info}"
            
        return result

class ConversationHandler:
    """
    Handler for multi-step conversations with users
    """
    
    def __init__(self, client, event, timeout: int = 300):
        """
        Initialize conversation handler
        
        Args:
            client: Telegram client
            event: Initial event that started the conversation
            timeout: Conversation timeout in seconds
        """
        self.client = client
        self.initial_event = event
        self.timeout = timeout
        self.last_activity = time.time()
        self.responses = {}
        self.current_state = None
        self.states = {}
        self.state_data = {}
        self.exit_handlers = []
    
    def add_state(self, name: str, prompt: str, 
                 handler: Callable, 
                 next_state: Union[str, Callable] = None,
                 timeout: int = None):
        """
        Add a state to the conversation flow
        
        Args:
            name: State name
            prompt: Message to send to user
            handler: Function to handle user response
            next_state: Next state name or function to determine next state
            timeout: State-specific timeout
        """
        self.states[name] = {
            "prompt": prompt,
            "handler": handler,
            "next_state": next_state,
            "timeout": timeout or self.timeout
        }
        
        return self
    
    def add_exit_handler(self, handler: Callable):
        """
        Add a handler to be called when conversation ends
        
        Args:
            handler: Function to call on exit
        """
        self.exit_handlers.append(handler)
        return self
    
    async def start(self, initial_state: str):
        """
        Start the conversation flow
        
        Args:
            initial_state: Name of the initial state
            
        Returns:
            Final conversation data
        """
        self.current_state = initial_state
        
        # Loop until conversation ends
        while self.current_state:
            state_info = self.states.get(self.current_state)
            if not state_info:
                logger.error(f"Invalid state: {self.current_state}")
                break
            
            # Send prompt for this state
            if callable(state_info["prompt"]):
                prompt = state_info["prompt"](self.state_data)
            else:
                prompt = state_info["prompt"]
            
            # Send the prompt message
            if hasattr(self.initial_event, "respond"):
                prompt_msg = await self.initial_event.respond(prompt)
            else:
                prompt_msg = await self.client.send_message(
                    self.initial_event.chat_id, prompt
                )
            
            # Wait for user response
            try:
                response = await self._wait_for_response(prompt_msg.chat_id, state_info["timeout"])
                
                # Process the response
                result = await state_info["handler"](response, self.state_data)
                
                # Store the result
                self.responses[self.current_state] = response
                
                # Update state data
                if isinstance(result, dict):
                    self.state_data.update(result)
                
                # Determine next state
                if callable(state_info["next_state"]):
                    self.current_state = state_info["next_state"](result, self.state_data)
                else:
                    self.current_state = state_info["next_state"]
                    
            except asyncio.TimeoutError:
                if hasattr(self.initial_event, "respond"):
                    await self.initial_event.respond("Conversation timed out. Please start over.")
                else:
                    await self.client.send_message(
                        self.initial_event.chat_id, "Conversation timed out. Please start over."
                    )
                break
        
        # Call exit handlers
        for handler in self.exit_handlers:
            await handler(self.state_data)
        
        return self.state_data
    
    async def _wait_for_response(self, chat_id, timeout):
        """
        Wait for a user response
        
        Args:
            chat_id: Chat ID to listen to
            timeout: Timeout in seconds
            
        Returns:
            User response event
            
        Raises:
            asyncio.TimeoutError: If no response received within timeout
        """
        # Implementation depends on client library (Telethon/Pyrogram)
        # This example assumes Telethon
        
        future = asyncio.Future()
        
        @self.client.on(events.NewMessage(chats=chat_id))
        async def handler(event):
            future.set_result(event)
            
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self.client.remove_event_handler(handler)

def create_robust_button(text, data, log_prefix="Button"):
    """
    Creates a button with comprehensive error handling to ensure buttons always display
    
    Args:
        text (str): Button text to display
        data (str): Callback data for the button
        log_prefix (str): Prefix for log messages for easier debugging
        
    Returns:
        Button: A properly configured Button object, or a fallback button if errors occur
    """
    try:
        # Input validation
        if not text or not isinstance(text, str):
            logger.warning(f"{log_prefix}: Invalid button text: {text}, using fallback")
            text = "Button" # Fallback text
            
        if not data or not isinstance(data, str):
            logger.warning(f"{log_prefix}: Invalid button data: {data}, using fallback")
            data = "error_button" # Fallback data
            
        # Check text length (Telegram has limits)
        if len(text) > 64:
            logger.warning(f"{log_prefix}: Button text too long, truncating: {text}")
            text = text[:61] + "..."
            
        # Ensure data is properly encoded
        try:
            # For special characters handling
            return Button.inline(text, data=data)
        except Exception as encode_error:
            logger.error(f"{log_prefix}: Error with button data '{data}': {str(encode_error)}")
            # Use a safe fallback
            return Button.inline(text, data="error_button")
            
    except Exception as e:
        # Catch-all for any other errors
        error_details = traceback.format_exc()
        logger.error(f"{log_prefix}: Critical button creation error: {str(e)}\n{error_details}")
        
        # Return a guaranteed-to-work fallback button
        try:
            return Button.inline("Menu", "main_menu")
        except:
            # Last resort fallback
            logger.critical(f"{log_prefix}: Ultimate button creation failure")
            return None
            
def create_button_grid(button_data, log_prefix="ButtonGrid"):
    """
    Creates a grid of buttons with error handling
    
    Args:
        button_data (list): List of button rows, where each row is a list of (text, data) tuples
        log_prefix (str): Prefix for log messages
        
    Returns:
        list: A list of button rows ready to be used in message methods
    """
    try:
        result = []
        for row_index, row in enumerate(button_data):
            button_row = []
            for btn_index, (btn_text, btn_data) in enumerate(row):
                try:
                    # Create each button with robust handler
                    btn = create_robust_button(
                        btn_text, 
                        btn_data, 
                        f"{log_prefix}[{row_index},{btn_index}]"
                    )
                    if btn:
                        button_row.append(btn)
                except Exception as btn_error:
                    logger.error(f"{log_prefix}: Error creating button at [{row_index},{btn_index}]: {str(btn_error)}")
                    # Add fallback button if the specific one fails
                    fallback = create_robust_button(f"Button {btn_index+1}", "error_button")
                    if fallback:
                        button_row.append(fallback)
            
            # Only add non-empty rows
            if button_row:
                result.append(button_row)
                
        # If we somehow end up with no buttons, add a fallback row
        if not result:
            logger.warning(f"{log_prefix}: No buttons were successfully created, adding fallback")
            fallback_btn = create_robust_button("Back to Menu", "main_menu")
            if fallback_btn:
                result.append([fallback_btn])
                
        return result
    except Exception as e:
        # Complete failure case
        error_details = traceback.format_exc()
        logger.error(f"{log_prefix}: Failed to create button grid: {str(e)}\n{error_details}")
        
        # Return minimal fallback
        try:
            fallback_btn = create_robust_button("Emergency Menu", "main_menu")
            if fallback_btn:
                return [[fallback_btn]]
            return []
        except:
            logger.critical(f"{log_prefix}: Complete button grid creation failure")
            return []

async def send_with_buttons(client, event, text, buttons, edit=False, log_prefix="ButtonMessage"):
    """
    Sends or edits a message with buttons, handling all potential errors
    
    Args:
        client: Telethon client
        event: The event to respond to
        text (str): Message text
        buttons (list): Button grid created with create_button_grid
        edit (bool): Whether to edit existing message or send new
        log_prefix (str): Prefix for log messages
        
    Returns:
        Message or None: The sent message object or None if failed
    """
    try:
        # Validate input
        if not text or not isinstance(text, str):
            logger.warning(f"{log_prefix}: Invalid message text, using fallback")
            text = "Select an option:"
            
        # Make sure we have buttons
        if not buttons or not isinstance(buttons, list):
            logger.warning(f"{log_prefix}: Invalid buttons parameter, creating fallback")
            fallback_btn = create_robust_button("Back to Menu", "main_menu")
            if fallback_btn:
                buttons = [[fallback_btn]]
            else:
                buttons = []
                
        # Try to send/edit the message
        if edit and hasattr(event, 'edit'):
            return await event.edit(text, buttons=buttons)
        else:
            return await event.respond(text, buttons=buttons)
            
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"{log_prefix}: Error sending message with buttons: {str(e)}\n{error_details}")
        
        # Try a simpler approach as fallback
        try:
            # Simplify - try without any formatting in the text
            simple_text = text.replace('**', '').replace('__', '').replace('`', '')
            
            # Try with simplified buttons
            simple_buttons = [[create_robust_button("Menu", "main_menu")]]
            
            if edit and hasattr(event, 'edit'):
                return await event.edit(simple_text, buttons=simple_buttons)
            else:
                return await event.respond(simple_text, buttons=simple_buttons)
                
        except Exception as fallback_error:
            logger.error(f"{log_prefix}: Fallback message also failed: {str(fallback_error)}")
            
            # Last resort - try to send a plain message without buttons
            try:
                emergency_text = "⚠️ Display error encountered. Use .start to restart."
                if edit and hasattr(event, 'edit'):
                    return await event.edit(emergency_text)
                else:
                    return await event.respond(emergency_text)
            except:
                logger.critical(f"{log_prefix}: Complete message sending failure")
                return None

# Initialize last update time
update_progress.last_update_time = 0

# Example of a conversation flow for adding users to groups
async def start_add_users_conversation(client, event):
    """
    Start a conversation flow for adding users to groups
    
    Args:
        client: Telegram client
        event: Event that triggered this conversation
        
    Returns:
        Conversation results
    """
    conversation = ConversationHandler(client, event)
    
    # Define handlers for each state
    async def handle_groups_selection(event, data):
        text = event.text.strip()
        groups = [g.strip() for g in text.split("\n") if g.strip()]
        return {"groups": groups}
    
    async def handle_users_selection(event, data):
        text = event.text.strip()
        users = [u.strip() for u in text.split("\n") if u.strip()]
        return {"users": users}
    
    async def handle_confirmation(event, data):
        text = event.text.lower().strip()
        return {"confirmed": text in ["yes", "y", "confirm", "ok"]}
    
    # Define state transitions
    def after_confirmation(result, data):
        if data.get("confirmed"):
            return "processing"
        else:
            return None  # End conversation
    
    # Add states to conversation
    conversation.add_state(
        "groups_selection",
        "Please list the groups you want to add users to (one per line):",
        handle_groups_selection,
        "users_selection"
    )
    
    conversation.add_state(
        "users_selection",
        "Please list the users you want to add (one per line):",
        handle_users_selection,
        "confirmation"
    )
    
    conversation.add_state(
        "confirmation",
        lambda data: f"You're about to add {len(data['users'])} users to {len(data['groups'])} groups.\nTotal operations: {len(data['users']) * len(data['groups'])}.\nConfirm? (yes/no)",
        handle_confirmation,
        after_confirmation
    )
    
    conversation.add_state(
        "processing",
        "Processing your request...",
        lambda event, data: data,
        None
    )
    
    # Add exit handler
    async def on_conversation_end(data):
        if data.get("confirmed"):
            await event.respond("Operation completed!")
        else:
            await event.respond("Operation cancelled.")
    
    conversation.add_exit_handler(on_conversation_end)
    
    # Start the conversation
    return await conversation.start("groups_selection")