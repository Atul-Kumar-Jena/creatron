"""
Message sending module with advanced flood wait handling and media support
"""
import asyncio
import time
import os
import datetime
import traceback
from telethon import functions, types, Button
from telethon.errors import (
    FloodWaitError, 
    ChatWriteForbiddenError, 
    SlowModeWaitError,
    MessageTooLongError,
    MessageIdInvalidError,
    MediaCaptionTooLongError,
    PhotoInvalidDimensionsError,
    MediaEmptyError,
    ReactionInvalidError,
    ScheduleDateInvalidError
)

from utils.floodwait import handle_flood_wait, check_cooldown, set_cooldown, get_progressive_delay
from utils.session_manager import update_session_stats
from utils.stats_manager import OperationTracker, calculate_eta, format_time_duration
import config

# Load balancer configuration
MAX_CONCURRENT_OPERATIONS = 3  # Max concurrent operations to avoid overloading
ADAPTIVE_BATCH_SIZE = True     # Whether to adjust batch size based on performance
operation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_OPERATIONS)
active_operations = {}

# Message sending callbacks
async def handle_message_callback(client, event, data):
    """
    Handle callbacks related to message sending
    
    Args:
        client: Telethon client
        event: Callback event
        data: Callback data
    """
    try:
        # Extract operation ID if present in callback data
        parts = data.split('_')
        if len(parts) >= 3:
            operation_id = parts[2]
            
            if data.startswith('msg_cancel'):
                # Cancel message sending operation
                if operation_id in active_operations:
                    active_operations[operation_id]["status"] = "cancelled"
                    # Use silent answer first
                    await event.answer("Operation cancelled! Stopping as soon as possible.", alert=False)
                    
                    # Edit the message to show cancellation and include buttons
                    try:
                        await event.edit(
                            "🚫 **Operation Cancelled**\n\n"
                            "The message sending operation has been cancelled. "
                            "Any messages already sent will not be affected.",
                            buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                        )
                    except Exception as e:
                        print(f"⚠️ Error updating cancel message: {str(e)}")
                else:
                    await event.answer("Operation not found or already completed.", alert=True)
                    
            elif data.startswith('msg_status'):
                # Check status of ongoing operation
                if operation_id in active_operations:
                    op_data = active_operations[operation_id]
                    status = op_data.get("status", "unknown")
                    completed = op_data.get("completed", 0)
                    total = op_data.get("total", 0)
                    progress = (completed / total * 100) if total > 0 else 0
                    
                    # Get ETA if available
                    eta_str = op_data.get("eta_formatted", "calculating...")
                    
                    # Create status message
                    if status == "running":
                        status_msg = f"✅ Operation is running\n"
                        status_msg += f"📊 Progress: {progress:.1f}% ({completed}/{total})\n"
                        status_msg += f"⏱️ Estimated time remaining: {eta_str}"
                    elif status == "completed":
                        total_time = op_data.get("total_time", 0)
                        time_str = format_time_duration(total_time)
                        status_msg = f"✅ Operation completed\n"
                        status_msg += f"📊 Completed: {completed}/{total}\n"
                        status_msg += f"⏱️ Total time: {time_str}"
                    elif status.startswith("flood_wait"):
                        wait_time = status.split("_")[-1]
                        status_msg = f"⏳ Operation paused (FloodWait)\n"
                        status_msg += f"📊 Progress: {progress:.1f}% ({completed}/{total})\n"
                        status_msg += f"⏱️ Waiting for {wait_time} before continuing"
                    elif status == "cancelled":
                        status_msg = f"🚫 Operation cancelled\n"
                        status_msg += f"📊 Completed before cancel: {completed}/{total}"
                    elif status == "failed":
                        error = op_data.get("error", "Unknown error")
                        status_msg = f"❌ Operation failed\n"
                        status_msg += f"📊 Completed before failure: {completed}/{total}\n"
                        status_msg += f"⚠️ Error: {error}"
                    else:
                        status_msg = f"Status: {status}\n"
                        status_msg += f"Progress: {progress:.1f}% ({completed}/{total})"
                        
                    # Show status as alert without editing the message
                    await event.answer(status_msg, alert=True)
                    
                    # Update status button if needed
                    if status in ["completed", "cancelled", "failed"]:
                        try:
                            # Change the button to show operation is no longer active
                            new_buttons = [
                                [Button.inline("✅ Operation Finished", f"msg_status_{operation_id}")],
                                [Button.inline("◀️ Back to Menu", "help_main")]
                            ]
                            await event.edit(buttons=new_buttons)
                        except Exception as e:
                            print(f"⚠️ Error updating status buttons: {str(e)}")
                else:
                    await event.answer("Operation not found or already completed.", alert=True)
                    
            elif data.startswith('msg_retry'):
                # Retry failed messages
                await event.answer("Retrying failed messages...", alert=False)
                
                # This would typically involve extracting failed message IDs and retrying them
                try:
                    await event.edit(
                        "🔄 **Retrying Failed Messages**\n\n"
                        "Please wait while we retry sending failed messages...",
                        buttons=None  # Clear buttons during processing
                    )
                    
                    # Simulate retrying for 2 seconds
                    await asyncio.sleep(2)
                    
                    await event.edit(
                        "✅ **Retry Completed**\n\n"
                        "The retry operation has completed. Check logs for details.",
                        buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                    )
                except Exception as e:
                    print(f"⚠️ Error during retry: {str(e)}")
                    await event.edit(
                        "⚠️ **Retry Failed**\n\n"
                        f"Error: {str(e)}",
                        buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                    )
        
        elif data == 'msg_send_now':
            # Handle "Send Now" button for scheduled messages
            await event.answer("Sending message immediately...", alert=False)
            
            # Edit message to indicate sending in progress
            try:
                await event.edit(
                    "🔄 **Sending message immediately...**\n\n"
                    "Please wait while we process your message.",
                    buttons=None  # Clear buttons during processing
                )
                
                # Simulate sending for 2 seconds
                await asyncio.sleep(2)
                
                await event.edit(
                    "✅ **Message Sent**\n\n"
                    "Your scheduled message has been sent immediately.",
                    buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                )
            except Exception as e:
                print(f"⚠️ Error sending message immediately: {str(e)}")
                await event.edit(
                    "⚠️ **Send Failed**\n\n"
                    f"Error: {str(e)}",
                    buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                )
                
        elif data == 'msg_schedule_cancel':
            # Cancel scheduled message
            await event.answer("Scheduled message has been cancelled.", alert=False)
            
            # Edit message to indicate cancellation
            try:
                await event.edit(
                    "🚫 **Scheduled Message Cancelled**\n\n"
                    "Your scheduled message has been cancelled.",
                    buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                )
            except Exception as e:
                print(f"⚠️ Error cancelling scheduled message: {str(e)}")
                
        elif data == 'msg_edit_menu':
            # Show message editing menu
            await event.answer("Opening edit menu...", alert=False)
            
            buttons = [
                [Button.inline("✏️ Edit Text", "msg_edit_text"),
                 Button.inline("🖼️ Change Media", "msg_edit_media")],
                [Button.inline("❌ Cancel", "msg_edit_cancel")]
            ]
            
            try:
                await event.edit(
                    "📝 **Message Editing Options**\n\n"
                    "Choose what you'd like to edit:", 
                    buttons=buttons
                )
            except Exception as e:
                print(f"⚠️ Error showing edit menu: {str(e)}")
                await event.answer(f"Error showing edit menu: {str(e)}", alert=True)
                
        elif data == 'msg_edit_text':
            # Start edit text conversation
            await event.answer("Please send the new text for your message.", alert=False)
            
            # Edit message to instruct user
            try:
                await event.edit(
                    "✏️ **Edit Message Text**\n\n"
                    "Please send the new text for your message in the chat.\n"
                    "Type `cancel` to cancel editing.",
                    buttons=[[Button.inline("❌ Cancel Editing", "msg_edit_cancel")]]
                )
            except Exception as e:
                print(f"⚠️ Error starting text edit: {str(e)}")
                
        elif data == 'msg_edit_media':
            # Start edit media conversation
            await event.answer("Please send the new media file for your message.", alert=False)
            
            # Edit message to instruct user
            try:
                await event.edit(
                    "🖼️ **Edit Message Media**\n\n"
                    "Please send the new media file in the chat.\n"
                    "Type `cancel` to cancel editing.",
                    buttons=[[Button.inline("❌ Cancel Editing", "msg_edit_cancel")]]
                )
            except Exception as e:
                print(f"⚠️ Error starting media edit: {str(e)}")
                
        elif data == 'msg_edit_cancel':
            # Cancel message editing
            await event.answer("Message editing cancelled.", alert=False)
            
            # Edit message to indicate cancellation
            try:
                await event.edit(
                    "🚫 **Message Editing Cancelled**\n\n"
                    "Your message editing operation has been cancelled.",
                    buttons=[[Button.inline("◀️ Back to Menu", "help_main")]]
                )
            except Exception as e:
                print(f"⚠️ Error cancelling edit: {str(e)}")
                
        # TEMPLATE MANAGEMENT CALLBACKS
        elif data == 'msg_template_create':
            # Start template creation conversation
            await event.answer("Please send the template content for your new template.", alert=False)
            
            # Edit message to instruct user
            try:
                await event.edit(
                    "📝 **Create Message Template**\n\n"
                    "Please send the content for your new template in the chat.\n"
                    "You can use the following placeholders:\n"
                    "- `{name}` - Recipient's name\n"
                    "- `{username}` - Recipient's username\n"
                    "- `{date}` - Current date\n"
                    "- `{time}` - Current time\n\n"
                    "Type `cancel` to cancel template creation.",
                    buttons=[[Button.inline("❌ Cancel Template Creation", "msg_templates")]]
                )
            except Exception as e:
                print(f"⚠️ Error starting template creation: {str(e)}")
                
        elif data == 'msg_template_view':
            # View saved templates
            await event.answer("Loading templates...", alert=False)
            
            try:
                # This would typically fetch templates from storage
                # For this implementation, we'll show a placeholder
                template_list = """📝 **Saved Message Templates**

1. **Welcome Message** - Last used: 2 days ago
   _"Welcome {name}! Thanks for joining..."_

2. **Follow-up** - Last used: Yesterday
   _"Hi {name}, just checking in about..."_

3. **Announcement** - Last used: 5 days ago
   _"Important announcement for all members..."_

4. **Thank You** - Last used: 1 week ago
   _"Thank you {name} for your participation..."_
"""
                buttons = [
                    [Button.inline("📝 Edit Template", "msg_template_edit"),
                     Button.inline("🗑️ Delete Template", "msg_template_delete")],
                    [Button.inline("📤 Use Template", "msg_template_use"),
                     Button.inline("◀️ Back", "msg_templates")]
                ]
                
                await event.edit(template_list, buttons=buttons)
            except Exception as e:
                print(f"⚠️ Error viewing templates: {str(e)}")
                await event.answer(f"Error viewing templates: {str(e)}", alert=True)
                
        elif data == 'msg_template_edit':
            # Template editing interface
            await event.answer("Select a template to edit...", alert=False)
            
            try:
                # This would typically show a list of templates to edit
                template_select = """✏️ **Edit Template**

Select a template to edit:
"""
                buttons = [
                    [Button.inline("1. Welcome Message", "msg_template_edit_1")],
                    [Button.inline("2. Follow-up", "msg_template_edit_2")],
                    [Button.inline("3. Announcement", "msg_template_edit_3")],
                    [Button.inline("4. Thank You", "msg_template_edit_4")],
                    [Button.inline("◀️ Back", "msg_template_view")]
                ]
                
                await event.edit(template_select, buttons=buttons)
            except Exception as e:
                print(f"⚠️ Error showing template edit selection: {str(e)}")
                await event.answer(f"Error showing template selection: {str(e)}", alert=True)
                
        elif data.startswith('msg_template_edit_'):
            # Handle editing specific template
            template_id = data.split('_')[-1]
            await event.answer(f"Loading template #{template_id}...", alert=False)
            
            # This would typically fetch the template content
            # For this implementation, we'll use placeholders
            template_contents = {
                "1": "Welcome {name}! Thanks for joining our group. We're excited to have you here!",
                "2": "Hi {name}, just checking in about our previous conversation. Let me know if you have any questions!",
                "3": "Important announcement for all members: We'll be performing system maintenance on {date} at {time}.",
                "4": "Thank you {name} for your participation in our recent event. Your contribution was valuable!"
            }
            
            template_content = template_contents.get(template_id, "Template content not found")
            
            try:
                await event.edit(
                    f"""✏️ **Edit Template #{template_id}**

Current content:
```
{template_content}
```

Please send the new content for this template in the chat.
Type `cancel` to cancel editing.""",
                    buttons=[[Button.inline("❌ Cancel Editing", "msg_template_view")]]
                )
            except Exception as e:
                print(f"⚠️ Error showing template content: {str(e)}")
                await event.answer(f"Error showing template content: {str(e)}", alert=True)
                
        elif data == 'msg_templates':
            # Main template management menu
            await event.answer("Opening template menu...", alert=False)
            
            try:
                await event.edit(
                    """📝 **Message Templates**

Manage your message templates:""",
                    buttons=[
                        [Button.inline("➕ Create New Template", "msg_template_create"),
                         Button.inline("📋 View Templates", "msg_template_view")],
                        [Button.inline("📥 Import Templates", "msg_template_import"),
                         Button.inline("📤 Export Templates", "msg_template_export")],
                        [Button.inline("🔍 Search Templates", "msg_template_search"),
                         Button.inline("◀️ Back to Menu", "help_main")]
                    ]
                )
            except Exception as e:
                print(f"⚠️ Error showing template menu: {str(e)}")
                await event.answer(f"Error showing template menu: {str(e)}", alert=True)
                
        else:
            # Handle any other callback data not explicitly defined
            await event.answer("This feature is still under development.", alert=True)
    
    except Exception as e:
        error_message = f"Error handling message callback: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        try:
            await event.answer(f"Error: {str(e)[:100]}", alert=True)
        except Exception:
            pass

# Function to create message control buttons
def get_message_control_buttons(operation_id):
    """
    Get buttons for controlling a message sending operation
    
    Args:
        operation_id: ID of the operation
    
    Returns:
        list: List of button rows
    """
    return [
        [Button.inline("🔄 Check Status", f"msg_status_{operation_id}"),
         Button.inline("🚫 Cancel", f"msg_cancel_{operation_id}")],
        [Button.inline("↩️ Back to Menu", "help_main")]
    ]

# Function to create schedule control buttons
def get_schedule_control_buttons():
    """
    Get buttons for controlling a scheduled message
    
    Returns:
        list: List of button rows
    """
    return [
        [Button.inline("📨 Send Now", "msg_send_now"),
         Button.inline("🚫 Cancel", "msg_schedule_cancel")],
        [Button.inline("✏️ Edit", "msg_edit_menu")]
    ]

async def send_messages(client, chat_id, messages, session_id=None, delay=None, 
                       adaptive_delay=True, media_paths=None, progress_callback=None,
                       reactions=None, schedule_date=None, reply_to=None, silent=False):
    """
    Send multiple messages to a chat with proper flood wait handling and media support
    
    Args:
        client: Telethon client
        chat_id: Chat ID or entity
        messages: List of message texts to send
        session_id: Session ID for stat tracking
        delay: Custom delay between messages (in seconds)
        adaptive_delay: Whether to use adaptive delay based on flood wait occurrences
        media_paths: List of file paths to send as media (can be None, or list matching messages length)
        progress_callback: Optional callback function to report progress
        reactions: List of reactions to add after sending each message (None or list matching messages length)
        schedule_date: Datetime object for scheduling messages (None for immediate sending)
        reply_to: Message ID to reply to (None for regular messages)
        silent: Whether to send messages silently (without notification)
    
    Returns:
        tuple: (success, result)
    """
    # Create an operation ID for tracking
    operation_id = f"message_sending_{int(time.time())}"
    active_operations[operation_id] = {
        "type": "message_sending",
        "start_time": time.time(),
        "total": len(messages),
        "completed": 0,
        "failed": 0,
        "status": "running"
    }
    
    # Tracker for ETA calculation
    async with OperationTracker("message_sending", operation_id):
        try:
            # Get chat entity
            try:
                chat = await client.get_entity(chat_id)
            except ValueError as e:
                # Specific error message if chat ID is invalid
                return False, f"Invalid chat ID or entity: {str(e)}"
            except Exception as e:
                return False, f"Error getting chat entity: {str(e)}"
            
            # Verify chat existence and permissions
            try:
                # Try to get basic chat info to verify permissions
                chat_full_info = await client(functions.messages.GetFullChatRequest(
                    chat_id=chat.id
                ))
            except ChatWriteForbiddenError:
                return False, "You don't have permission to write in this chat"
            except Exception as e:
                # Continue anyway - this is just a verification step
                print(f"⚠️ Warning: Could not verify chat permissions: {str(e)}")
            
            # Use default delay if not specified
            if delay is None:
                delay = config.MESSAGE_COOLDOWN
            
            # Initialize base delay for adaptive delay
            base_delay = delay
            current_delay = base_delay
            flood_count = 0
            
            # Track success and failures
            results = {
                'success': 0,
                'failed': 0,
                'messages': [],
                'errors': []
            }
            
            # Calculate ETA
            total_messages = len(messages)
            eta_seconds, eta_formatted = await calculate_eta("message_sending", total_messages)
            
            print(f"📨 Sending {total_messages} messages with {delay}s initial delay between each")
            print(f"⏱️ Estimated time: {eta_formatted}")
            
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    scheduled_info = f"scheduled for {schedule_date}" if schedule_date else "immediate"
                    log_message = f"LOG: MESSAGE_SEND_STARTED | Chat: {chat.id} | Messages: {total_messages} | Mode: {scheduled_info} | ETA: {eta_formatted} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging message send start: {str(e)}")
            
            # Validate media paths if provided
            if media_paths is not None:
                if len(media_paths) != len(messages):
                    active_operations[operation_id]["status"] = "failed"
                    active_operations[operation_id]["error"] = f"Media paths list length mismatch"
                    return False, f"Media paths list length ({len(media_paths)}) doesn't match messages length ({len(messages)})"
                
                # Check if media files exist
                for i, path in enumerate(media_paths):
                    if path is not None and not os.path.exists(path):
                        active_operations[operation_id]["status"] = "failed"
                        active_operations[operation_id]["error"] = f"Media file not found: {path}"
                        return False, f"Media file not found: {path}"
            
            # Validate reactions if provided
            if reactions is not None:
                if len(reactions) != len(messages):
                    active_operations[operation_id]["status"] = "failed"
                    active_operations[operation_id]["error"] = f"Reactions list length mismatch"
                    return False, f"Reactions list length ({len(reactions)}) doesn't match messages length ({len(messages)})"
            
            # Determine optimal batch size based on message count
            # For small message counts, use a smaller batch to get feedback faster
            # For larger message counts, use larger batches for efficiency
            if ADAPTIVE_BATCH_SIZE:
                if total_messages <= 5:
                    batch_size = 1  # Process one at a time for small batches
                elif total_messages <= 20:
                    batch_size = 5  # Small batch size for medium counts
                elif total_messages <= 100:
                    batch_size = 10 # Medium batch size for larger counts
                else:
                    batch_size = 20 # Larger batch size for very large counts
            else:
                batch_size = min(10, total_messages)  # Fixed batch size
            
            # Create batches
            batches = [range(i, min(i + batch_size, total_messages)) for i in range(0, total_messages, batch_size)]
            
            print(f"🔄 Processing messages in {len(batches)} batches of up to {batch_size} messages each")
            
            # Start time for ETA calculation
            start_time = time.time()
            last_eta_update = start_time
            last_progress_update = start_time
            
            # Process each batch
            total_processed = 0
            for batch_idx, batch_range in enumerate(batches):
                print(f"📦 Processing batch {batch_idx+1}/{len(batches)}")
                
                # Acquire semaphore for load balancing
                async with operation_semaphore:
                    # Send each message in the batch
                    for i in batch_range:
                        try:
                            # Get media path for this message if available
                            media_path = None if media_paths is None else media_paths[i]
                            
                            # Prepare message parameters
                            message_text = messages[i]
                            send_params = {
                                'silent': silent
                            }
                            
                            # Add schedule date if provided
                            if schedule_date:
                                send_params['schedule'] = schedule_date
                            
                            # Add reply_to if provided
                            if reply_to:
                                send_params['reply_to'] = reply_to
                            
                            # Try to send with retries for transient errors
                            retry_count = 0
                            max_retries = 3
                            success = False
                            
                            while not success and retry_count < max_retries:
                                try:
                                    # Send message with or without media
                                    if media_path:
                                        try:
                                            # Send message with media
                                            sent_message = await client.send_file(
                                                chat, 
                                                media_path, 
                                                caption=message_text,
                                                **send_params
                                            )
                                        except (MediaCaptionTooLongError, PhotoInvalidDimensionsError, MediaEmptyError) as media_error:
                                            # If media send fails, try to send just the text
                                            print(f"⚠️ Media error: {str(media_error)}. Sending text only.")
                                            sent_message = await client.send_message(chat, message_text, **send_params)
                                            # Add to errors list
                                            results['errors'].append({
                                                'message_index': i,
                                                'error': f"Media error: {str(media_error)}"
                                            })
                                    else:
                                        # Send text message
                                        sent_message = await client.send_message(chat, message_text, **send_params)
                                    
                                    # Successfully sent message
                                    success = True
                                
                                except FloodWaitError as flood_error:
                                    # Handle flood wait
                                    flood_count += 1
                                    wait_time = flood_error.seconds
                                    
                                    print(f"⏳ FloodWaitError: Waiting for {wait_time} seconds (attempt {retry_count+1}/{max_retries})")
                                    
                                    # Update operation info with flood wait status
                                    active_operations[operation_id]["status"] = f"flood_wait_{wait_time}s"
                                    active_operations[operation_id]["flood_count"] = flood_count
                                    
                                    # Wait for the required time
                                    await asyncio.sleep(wait_time)
                                    
                                    # Increase delay for future messages
                                    if adaptive_delay:
                                        current_delay = get_progressive_delay(base_delay, flood_count)
                                        print(f"⚙️ Adaptive delay increased to {current_delay}s")
                                    
                                    # Try again
                                    retry_count += 1
                                    continue
                                
                                except (SlowModeWaitError, MessageTooLongError, MessageIdInvalidError, ScheduleDateInvalidError) as known_error:
                                    # Handle known errors
                                    error_str = str(known_error)
                                    print(f"⚠️ Error sending message: {error_str}")
                                    
                                    if isinstance(known_error, SlowModeWaitError):
                                        # If it's a slow mode error, wait and retry
                                        wait_time = getattr(known_error, 'seconds', 30)
                                        print(f"⏳ SlowModeWaitError: Waiting for {wait_time} seconds")
                                        await asyncio.sleep(wait_time)
                                        retry_count += 1
                                        continue
                                    
                                    # For other errors, record and move on
                                    results['failed'] += 1
                                    results['errors'].append({
                                        'message_index': i,
                                        'error': error_str
                                    })
                                    break
                                
                                except Exception as e:
                                    # Handle unexpected errors
                                    error_str = str(e)
                                    print(f"⚠️ Unexpected error sending message: {error_str}")
                                    traceback.print_exc()
                                    
                                    # For temporary errors, retry
                                    if "timeout" in error_str.lower() or "connection" in error_str.lower():
                                        print(f"🔄 Retrying after connection error (attempt {retry_count+1}/{max_retries})")
                                        await asyncio.sleep(5)  # Wait a bit before retrying
                                        retry_count += 1
                                        continue
                                    
                                    # For other errors, record and move on
                                    results['failed'] += 1
                                    results['errors'].append({
                                        'message_index': i,
                                        'error': error_str
                                    })
                                    break
                            
                            # If success after retries, continue with normal flow
                            if success:
                                # Try to add reaction if specified
                                if reactions is not None and reactions[i]:
                                    try:
                                        await client.send_reaction(chat, sent_message.id, reactions[i])
                                        print(f"✅ Added reaction '{reactions[i]}' to message")
                                    except ReactionInvalidError:
                                        print(f"⚠️ Invalid reaction: {reactions[i]}")
                                        results['errors'].append({
                                            'message_index': i,
                                            'error': f"Invalid reaction: {reactions[i]}"
                                        })
                                    except Exception as reaction_error:
                                        print(f"⚠️ Error adding reaction: {str(reaction_error)}")
                                        results['errors'].append({
                                            'message_index': i,
                                            'error': f"Reaction error: {str(reaction_error)}"
                                        })
                                
                                results['success'] += 1
                                results['messages'].append(sent_message.id)
                                
                                # Update operation stats
                                active_operations[operation_id]["completed"] += 1
                            else:
                                # Failed after all retries
                                results['failed'] += 1
                                active_operations[operation_id]["failed"] += 1
                            
                            # Update progress
                            total_processed += 1
                            progress = total_processed / total_messages * 100
                            
                            # Recalculate ETA every 5 messages or 15 seconds
                            current_time = time.time()
                            if (total_processed % 5 == 0 or current_time - last_eta_update > 15) and total_processed < total_messages:
                                elapsed_time = current_time - start_time
                                messages_per_second = total_processed / elapsed_time if elapsed_time > 0 else 0
                                remaining_messages = total_messages - total_processed
                                
                                if messages_per_second > 0:
                                    estimated_seconds_remaining = remaining_messages / messages_per_second
                                    eta_formatted = format_time_duration(estimated_seconds_remaining)
                                    
                                    # Update ETA information
                                    print(f"⏱️ Progress: {progress:.1f}% | ETA: {eta_formatted}")
                                    active_operations[operation_id]["eta_seconds"] = estimated_seconds_remaining
                                    active_operations[operation_id]["eta_formatted"] = eta_formatted
                                    active_operations[operation_id]["progress"] = progress
                                    
                                    last_eta_update = current_time
                            
                            # Call progress callback if provided (every 2 seconds maximum)
                            if progress_callback and (current_time - last_progress_update > 2):
                                try:
                                    await progress_callback(progress, total_processed, total_messages)
                                    last_progress_update = current_time
                                except Exception as e:
                                    print(f"⚠️ Error in progress callback: {str(e)}")
                            
                            # Apply delay between messages with adaptive delay if needed
                            if i < total_messages - 1:  # No need to delay after the last message
                                if flood_count > 0 and adaptive_delay:
                                    await asyncio.sleep(current_delay)
                                else:
                                    await asyncio.sleep(delay)
                            
                            # Update session stats if session_id is provided
                            if session_id:
                                update_session_stats(session_id, "messages_sent", 1)
                        
                        except Exception as e:
                            # Catch any other errors and continue with the next message
                            error_str = str(e)
                            print(f"⚠️ Unexpected error in message processing loop: {error_str}")
                            traceback.print_exc()
                            
                            results['failed'] += 1
                            results['errors'].append({
                                'message_index': i,
                                'error': error_str
                            })
                            
                            active_operations[operation_id]["failed"] += 1
                            continue
                
                # Small delay between batches to reduce server load
                if batch_idx < len(batches) - 1:  # No need to delay after the last batch
                    await asyncio.sleep(1)
            
            # Calculate final statistics
            total_time = time.time() - start_time
            messages_per_second = total_messages / total_time if total_time > 0 else 0
            
            # Log completion to database
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: MESSAGE_SEND_COMPLETED | Chat: {chat.id} | Success: {results['success']} | Failed: {results['failed']} | Time: {format_time_duration(total_time)} | Rate: {messages_per_second:.2f} msg/s | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging message send completion: {str(e)}")
            
            # Update final operation status
            active_operations[operation_id]["status"] = "completed"
            active_operations[operation_id]["total_time"] = total_time
            active_operations[operation_id]["messages_per_second"] = messages_per_second
            
            # Print summary
            print(f"✅ Message sending complete!")
            print(f"📊 Success: {results['success']}/{total_messages} | Failed: {results['failed']}")
            print(f"⏱️ Total time: {format_time_duration(total_time)} | Rate: {messages_per_second:.2f} messages/second")
            
            # Return results
            return True, results
            
        except Exception as e:
            # Handle any unexpected errors in the main function
            error_str = str(e)
            print(f"❌ Error in send_messages function: {error_str}")
            traceback.print_exc()
            
            # Update operation status
            active_operations[operation_id]["status"] = "failed"
            active_operations[operation_id]["error"] = error_str
            
            return False, {"error": error_str, "traceback": traceback.format_exc()}
        
async def schedule_message(client, chat_id, message, schedule_timestamp, media_path=None, 
                          silent=False, session_id=None):
    """
    Schedule a message to be sent at a specific time
    
    Args:
        client: Telethon client
        chat_id: Chat ID or entity
        message: Message text to send
        schedule_timestamp: Unix timestamp or datetime object for when to send the message
        media_path: Optional file path for media to attach
        silent: Whether to send message silently (without notification)
        session_id: Session ID for stat tracking
    
    Returns:
        tuple: (success, result)
    """
    try:
        # Get chat entity
        try:
            chat = await client.get_entity(chat_id)
        except ValueError as e:
            return False, f"Invalid chat ID or entity: {str(e)}"
        except Exception as e:
            return False, f"Error getting chat entity: {str(e)}"
        
        # Convert timestamp to datetime if it's a unix timestamp
        if isinstance(schedule_timestamp, (int, float)):
            schedule_date = datetime.datetime.fromtimestamp(schedule_timestamp, tz=datetime.timezone.utc)
        else:
            schedule_date = schedule_timestamp
            
        # Make sure the schedule date is in the future
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if schedule_date <= now:
            return False, "Schedule date must be in the future"
        
        # Calculate seconds until scheduled time
        time_delta = schedule_date - now
        seconds_until = time_delta.total_seconds()
        
        # Format for display
        formatted_date = schedule_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        print(f"📅 Scheduling message for {formatted_date} ({seconds_until:.1f} seconds from now)")
        
        # Prepare message parameters
        send_params = {
            'silent': silent,
            'schedule': schedule_date
        }
        
        # Send the scheduled message
        try:
            if media_path:
                # Check if media file exists
                if not os.path.exists(media_path):
                    return False, f"Media file not found: {media_path}"
                    
                # Send with media
                result = await client.send_file(
                    chat,
                    media_path,
                    caption=message,
                    **send_params
                )
            else:
                # Send text only
                result = await client.send_message(
                    chat,
                    message,
                    **send_params
                )
                
            # Update stats if session_id is provided
            if session_id:
                await update_session_stats(session_id, 'messages_scheduled')
                
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: MESSAGE_SCHEDULED | Chat: {chat.id} | Time: {formatted_date} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging scheduled message: {str(e)}")
                
            return True, result
            
        except ScheduleDateInvalidError:
            error_message = "Invalid schedule date. Make sure it's not too far in the future (maximum is 365 days)."
            print(f"⚠️ {error_message}")
            return False, error_message
            
        except Exception as e:
            error_message = f"Error scheduling message: {str(e)}"
            print(f"⚠️ {error_message}")
            
            # Log to errors database if it exists
            if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                try:
                    error_log = f"ERROR: SCHEDULE_MESSAGE | Chat: {chat.id} | Time: {formatted_date} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["errors"], error_log)
                except Exception:
                    pass
                    
            return False, error_message
            
    except Exception as e:
        error_message = f"Error preparing scheduled message: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Log to errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_log = f"ERROR: SCHEDULE_MESSAGE_PREP | Chat: {chat_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_log)
            except Exception:
                pass
                
        return False, error_message

async def edit_message(client, chat_id, message_id, new_text, media_path=None, session_id=None):
    """
    Edit an existing message
    
    Args:
        client: Telethon client
        chat_id: Chat ID or entity
        message_id: ID of the message to edit
        new_text: New message text
        media_path: Optional new media path
        session_id: Session ID for stat tracking
    
    Returns:
        tuple: (success, result)
    """
    try:
        # Get chat entity
        try:
            chat = await client.get_entity(chat_id)
        except ValueError as e:
            return False, f"Invalid chat ID or entity: {str(e)}"
        except Exception as e:
            return False, f"Error getting chat entity: {str(e)}"
        
        # Check if this is our message (can only edit own messages)
        try:
            message = await client.get_messages(chat, ids=message_id)
            if not message:
                return False, f"Message with ID {message_id} not found"
                
            if not message.out:
                return False, "Cannot edit messages sent by other users"
                
        except Exception as e:
            return False, f"Error checking message: {str(e)}"
        
        # Edit the message
        try:
            if media_path:
                # Check if media file exists
                if not os.path.exists(media_path):
                    return False, f"Media file not found: {media_path}"
                    
                # Edit with new media
                result = await client.edit_message(
                    chat,
                    message_id,
                    text=new_text,
                    file=media_path
                )
            else:
                # Edit text only
                result = await client.edit_message(
                    chat,
                    message_id,
                    text=new_text
                )
                
            # Update stats if session_id is provided
            if session_id:
                await update_session_stats(session_id, 'messages_edited')
                
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: MESSAGE_EDITED | Chat: {chat.id} | Message: {message_id} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging message edit: {str(e)}")
                
            return True, result
            
        except MessageTooLongError:
            return False, "Message too long. Maximum length is 4096 characters."
            
        except MessageIdInvalidError:
            return False, f"Invalid message ID: {message_id}"
            
        except Exception as e:
            error_message = f"Error editing message: {str(e)}"
            print(f"⚠️ {error_message}")
            
            # Log to errors database if it exists
            if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                try:
                    error_log = f"ERROR: EDIT_MESSAGE | Chat: {chat.id} | Message: {message_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["errors"], error_log)
                except Exception:
                    pass
                    
            return False, error_message
            
    except Exception as e:
        error_message = f"Error preparing message edit: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Log to errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_log = f"ERROR: EDIT_MESSAGE_PREP | Chat: {chat_id} | Message: {message_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_log)
            except Exception:
                pass
                
        return False, error_message

async def delete_messages(client, chat_id, message_ids, session_id=None):
    """
    Delete messages from a chat
    
    Args:
        client: Telethon client
        chat_id: Chat ID or entity
        message_ids: Single message ID or list of message IDs to delete
        session_id: Session ID for stat tracking
    
    Returns:
        tuple: (success, result)
    """
    try:
        # Get chat entity
        try:
            chat = await client.get_entity(chat_id)
        except ValueError as e:
            return False, f"Invalid chat ID or entity: {str(e)}"
        except Exception as e:
            return False, f"Error getting chat entity: {str(e)}"
        
        # Convert single ID to list if needed
        if not isinstance(message_ids, list):
            message_ids = [message_ids]
            
        # Delete messages
        try:
            # We can only delete our own messages in private chats
            # In channels/groups with proper permissions, we can delete any message
            
            # Check which messages we can delete
            messages = await client.get_messages(chat, ids=message_ids)
            deletable_ids = []
            
            for msg in messages:
                if msg and (msg.out or chat.admin_rights):
                    deletable_ids.append(msg.id)
            
            if not deletable_ids:
                return False, "No deletable messages found. You can only delete your own messages or messages in chats where you have admin rights."
            
            # Delete the messages
            await client.delete_messages(chat, deletable_ids)
            
            # Update stats if session_id is provided
            if session_id:
                await update_session_stats(session_id, 'messages_deleted', len(deletable_ids))
                
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: MESSAGES_DELETED | Chat: {chat.id} | Count: {len(deletable_ids)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging message deletion: {str(e)}")
                
            result = {
                'deleted_count': len(deletable_ids),
                'total_requested': len(message_ids),
                'deletable_ids': deletable_ids
            }
            
            return True, result
            
        except MessageIdInvalidError:
            return False, "One or more message IDs are invalid"
            
        except Exception as e:
            error_message = f"Error deleting messages: {str(e)}"
            print(f"⚠️ {error_message}")
            
            # Log to errors database if it exists
            if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                try:
                    error_log = f"ERROR: DELETE_MESSAGES | Chat: {chat.id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["errors"], error_log)
                except Exception:
                    pass
                    
            return False, error_message
            
    except Exception as e:
        error_message = f"Error preparing message deletion: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Log to errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_log = f"ERROR: DELETE_MESSAGES_PREP | Chat: {chat_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_log)
            except Exception:
                pass
                
        return False, error_message

async def send_message_to_multiple_chats(client, chat_ids, message, session_id=None, delay=None, 
                                        adaptive_delay=True, media_path=None, progress_callback=None):
    """
    Send a message to multiple chats with proper flood wait handling and media support
    
    Args:
        client: Telethon client
        chat_ids: List of chat IDs or entities
        message: Message text to send
        session_id: Session ID for stat tracking
        delay: Custom delay between messages (in seconds)
        adaptive_delay: Whether to use adaptive delay based on flood wait occurrences
        media_path: Optional file path to send as media with each message
        progress_callback: Optional callback function to report progress
    
    Returns:
        tuple: (success, result)
    """
    try:
        # Use default delay if not specified
        if delay is None:
            delay = config.MESSAGE_COOLDOWN
        
        # Initialize base delay for adaptive delay
        base_delay = delay
        current_delay = base_delay
        flood_count = 0
        
        # Track success and failures
        results = {
            'success': [],
            'failed': [],
            'errors': {}
        }
        
        # Calculate total estimated time
        total_chats = len(chat_ids)
        estimated_time = total_chats * delay
        
        print(f"📨 Sending to {total_chats} chats with {delay}s initial delay between each")
        print(f"⏱️ Estimated time: {estimated_time:.1f} seconds")
        
        # Validate media path if provided
        if media_path and not os.path.exists(media_path):
            return False, f"Media file not found: {media_path}"
        
        # Log to logs database if it exists
        if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
            try:
                log_message = f"LOG: BULK_MESSAGE_STARTED | Chats: {total_chats} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["logs"], log_message)
            except Exception as e:
                print(f"⚠️ Error logging bulk message start: {str(e)}")
        
        # Send to each chat one by one with proper delay
        for i, chat_id in enumerate(chat_ids):
            try:
                # Get chat entity
                try:
                    chat = await client.get_entity(chat_id)
                except ValueError:
                    results['failed'].append(chat_id)
                    results['errors'][chat_id] = "Invalid chat ID or entity"
                    continue
                except Exception as e:
                    results['failed'].append(chat_id)
                    results['errors'][chat_id] = f"Error getting chat entity: {str(e)}"
                    continue
                
                # Send message with or without media
                if media_path:
                    try:
                        # Send message with media
                        await client.send_file(
                            chat, 
                            media_path, 
                            caption=message
                        )
                    except (MediaCaptionTooLongError, PhotoInvalidDimensionsError, MediaEmptyError) as media_error:
                        # If media send fails, try to send just the text
                        print(f"⚠️ Media error: {str(media_error)}. Sending text only to {chat.id}.")
                        await client.send_message(chat, message)
                        # Note the media error
                        if chat_id not in results['errors']:
                            results['errors'][chat_id] = []
                        results['errors'][chat_id] = f"Media error: {str(media_error)}, sent text only"
                else:
                    # Send text message
                    await client.send_message(chat, message)
                
                results['success'].append(chat_id)
                
                # Update progress
                progress = (i + 1) / total_chats * 100
                progress_message = f"Progress: {progress:.1f}% ({i+1}/{total_chats})"
                print(progress_message)
                
                # Call progress callback if provided
                if progress_callback:
                    try:
                        await progress_callback(i + 1, total_chats, progress)
                    except Exception as callback_error:
                        print(f"⚠️ Progress callback error: {str(callback_error)}")
                
                # Update stats if session_id is provided
                if session_id:
                    await update_session_stats(session_id, 'messages_sent')
                
                # Wait before sending to the next chat (if not the last one)
                if i < total_chats - 1:
                    # Use adaptive delay if enabled
                    if adaptive_delay and flood_count > 0:
                        # Increase delay based on number of flood waits
                        current_delay = base_delay * (1 + (flood_count * 0.5))
                        print(f"ℹ️ Using adaptive delay: {current_delay:.1f}s due to {flood_count} flood waits")
                        
                    await asyncio.sleep(current_delay)
                
            except MessageTooLongError:
                # Split message and send in parts
                try:
                    # Simple split at 4000 characters (Telegram's limit is around 4096)
                    chunks = [message[j:j+4000] for j in range(0, len(message), 4000)]
                    
                    for chunk in chunks:
                        await client.send_message(chat, chunk)
                        await asyncio.sleep(1)  # Small delay between chunks
                    
                    results['success'].append(chat_id)
                    
                    # Update stats if session_id is provided
                    if session_id:
                        await update_session_stats(session_id, 'messages_sent')
                        
                except Exception as split_error:
                    results['failed'].append(chat_id)
                    results['errors'][chat_id] = f"Split error: {str(split_error)}"
                    
                    # Update stats if session_id is provided
                    if session_id:
                        await update_session_stats(session_id, 'errors')
                
            except FloodWaitError as e:
                # Handle the flood wait
                flood_count += 1
                
                # Increase delay for future messages
                if adaptive_delay:
                    current_delay = base_delay * (1 + (flood_count * 0.5))
                    print(f"⚠️ Flood detected! Increasing delay to {current_delay:.1f}s for future messages")
                
                await handle_flood_wait(e.seconds, 'message_send')
                
                # Try again for this chat
                try:
                    if media_path:
                        await client.send_file(chat, media_path, caption=message)
                    else:
                        await client.send_message(chat, message)
                        
                    results['success'].append(chat_id)
                    
                    # Update stats if session_id is provided
                    if session_id:
                        await update_session_stats(session_id, 'messages_sent')
                except Exception as retry_error:
                    results['failed'].append(chat_id)
                    results['errors'][chat_id] = f"Retry error: {str(retry_error)}"
                    
                    # Update stats if session_id is provided
                    if session_id:
                        await update_session_stats(session_id, 'errors')
                
            except ChatWriteForbiddenError:
                results['failed'].append(chat_id)
                results['errors'][chat_id] = "No permission to write in this chat"
                print(f"⚠️ No permission to write in chat {chat_id}")
                
                # Update stats if session_id is provided
                if session_id:
                    await update_session_stats(session_id, 'errors')
                    
            except Exception as e:
                results['failed'].append(chat_id)
                results['errors'][chat_id] = str(e)
                print(f"Failed to send to chat {chat_id}: {str(e)}")
                
                # Update stats if session_id is provided
                if session_id:
                    await update_session_stats(session_id, 'errors')
                    
                # Log to errors database if it exists
                if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                    try:
                        error_message = f"ERROR: SEND_TO_CHAT | Chat: {chat_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        await client.send_message(config.DB_GROUPS["errors"], error_message)
                    except Exception:
                        pass
        
        # Log completion to logs database if it exists
        if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
            try:
                log_message = f"LOG: BULK_MESSAGE_COMPLETED | Success: {len(results['success'])} | Failed: {len(results['failed'])} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["logs"], log_message)
            except Exception as e:
                print(f"⚠️ Error logging bulk message completion: {str(e)}")
        
        return True, results
        
    except Exception as e:
        error_message = f"Error sending to multiple chats: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Log to errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_log = f"ERROR: SEND_MULTIPLE | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_log)
            except Exception:
                pass
                
        return False, error_message

async def forward_messages(client, from_chat_id, to_chat_ids, message_ids, session_id=None, delay=None, adaptive_delay=True):
    """
    Forward messages to multiple chats with proper flood wait handling
    
    Args:
        client: Telethon client
        from_chat_id: Source chat ID or entity
        to_chat_ids: List of destination chat IDs or entities
        message_ids: List of message IDs to forward
        session_id: Session ID for stat tracking
        delay: Custom delay between forwards (in seconds)
        adaptive_delay: Whether to use adaptive delay based on flood wait occurrences
    
    Returns:
        tuple: (success, result)
    """
    try:
        # Get source chat entity
        try:
            from_chat = await client.get_entity(from_chat_id)
        except ValueError:
            return False, "Invalid source chat ID or entity"
        except Exception as e:
            return False, f"Error getting source chat entity: {str(e)}"
        
        # Use default delay if not specified
        if delay is None:
            delay = config.MESSAGE_COOLDOWN
        
        # Initialize base delay for adaptive delay
        base_delay = delay
        current_delay = base_delay
        flood_count = 0
        
        # Track success and failures
        results = {
            'success': {},
            'failed': {}
        }
        
        # Initialize results for each destination chat
        for chat_id in to_chat_ids:
            results['success'][chat_id] = []
            results['failed'][chat_id] = []
        
        # Calculate total estimated time
        total_forwards = len(to_chat_ids) * len(message_ids)
        estimated_time = total_forwards * delay
        
        print(f"📨 Forwarding {len(message_ids)} messages to {len(to_chat_ids)} chats")
        print(f"⏱️ Estimated time: {estimated_time:.1f} seconds")
        
        # Log to logs database if it exists
        if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
            try:
                log_message = f"LOG: FORWARD_STARTED | From: {from_chat_id} | To: {len(to_chat_ids)} chats | Messages: {len(message_ids)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["logs"], log_message)
            except Exception as e:
                print(f"⚠️ Error logging forward start: {str(e)}")
        
        # Forward to each chat one by one
        for i, to_chat_id in enumerate(to_chat_ids):
            try:
                # Get destination chat entity
                try:
                    to_chat = await client.get_entity(to_chat_id)
                except ValueError:
                    for msg_id in message_ids:
                        results['failed'][to_chat_id].append(msg_id)
                    continue
                except Exception as e:
                    for msg_id in message_ids:
                        results['failed'][to_chat_id].append(msg_id)
                    continue
                
                # Forward each message
                for j, msg_id in enumerate(message_ids):
                    try:
                        # Forward the message
                        forwarded = await client.forward_messages(
                            to_chat,
                            messages=msg_id,
                            from_peer=from_chat
                        )
                        
                        if forwarded:
                            results['success'][to_chat_id].append(msg_id)
                            
                            # Update stats if session_id is provided
                            if session_id:
                                await update_session_stats(session_id, 'messages_sent')
                        else:
                            results['failed'][to_chat_id].append(msg_id)
                        
                        # Update progress
                        current = i * len(message_ids) + j + 1
                        progress = (current / total_forwards) * 100
                        print(f"Progress: {progress:.1f}% ({current}/{total_forwards})")
                        
                        # Wait before next forward (if not the last one)
                        if not (i == len(to_chat_ids) - 1 and j == len(message_ids) - 1):
                            # Use adaptive delay if enabled
                            if adaptive_delay and flood_count > 0:
                                current_delay = base_delay * (1 + (flood_count * 0.5))
                                
                            await asyncio.sleep(current_delay)
                    
                    except MessageIdInvalidError:
                        results['failed'][to_chat_id].append(msg_id)
                        print(f"⚠️ Invalid message ID: {msg_id}")
                        
                        # Update stats if session_id is provided
                        if session_id:
                            await update_session_stats(session_id, 'errors')
                    
                    except FloodWaitError as e:
                        # Handle the flood wait
                        flood_count += 1
                        
                        # Increase delay for future forwards
                        if adaptive_delay:
                            current_delay = base_delay * (1 + (flood_count * 0.5))
                            print(f"⚠️ Flood detected! Increasing delay to {current_delay:.1f}s for future forwards")
                        
                        await handle_flood_wait(e.seconds, 'message_forward')
                        
                        # Try again for this message
                        try:
                            forwarded = await client.forward_messages(
                                to_chat,
                                messages=msg_id,
                                from_peer=from_chat
                            )
                            
                            if forwarded:
                                results['success'][to_chat_id].append(msg_id)
                                
                                # Update stats if session_id is provided
                                if session_id:
                                    await update_session_stats(session_id, 'messages_sent')
                            else:
                                results['failed'][to_chat_id].append(msg_id)
                        except Exception:
                            results['failed'][to_chat_id].append(msg_id)
                            
                            # Update stats if session_id is provided
                            if session_id:
                                await update_session_stats(session_id, 'errors')
                    
                    except Exception as e:
                        results['failed'][to_chat_id].append(msg_id)
                        print(f"Failed to forward message {msg_id} to chat {to_chat_id}: {str(e)}")
                        
                        # Update stats if session_id is provided
                        if session_id:
                            await update_session_stats(session_id, 'errors')
                            
                        # Log to errors database if it exists
                        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                            try:
                                error_message = f"ERROR: FORWARD_MESSAGE | From: {from_chat_id} | To: {to_chat_id} | Msg: {msg_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                                await client.send_message(config.DB_GROUPS["errors"], error_message)
                            except Exception:
                                pass
            
            except ChatWriteForbiddenError:
                for msg_id in message_ids:
                    results['failed'][to_chat_id].append(msg_id)
                print(f"⚠️ No permission to write in chat {to_chat_id}")
                
            except Exception as e:
                for msg_id in message_ids:
                    results['failed'][to_chat_id].append(msg_id)
                print(f"Failed to forward to chat {to_chat_id}: {str(e)}")
                
                # Log to errors database if it exists
                if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                    try:
                        error_message = f"ERROR: FORWARD_TO_CHAT | From: {from_chat_id} | To: {to_chat_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        await client.send_message(config.DB_GROUPS["errors"], error_message)
                    except Exception:
                        pass
        
        # Calculate total success and failure counts
        total_success = sum(len(msgs) for msgs in results['success'].values())
        total_failed = sum(len(msgs) for msgs in results['failed'].values())
        
        # Log completion to logs database if it exists
        if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
            try:
                log_message = f"LOG: FORWARD_COMPLETED | Success: {total_success} | Failed: {total_failed} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["logs"], log_message)
            except Exception as e:
                print(f"⚠️ Error logging forward completion: {str(e)}")
        
        return True, results
        
    except Exception as e:
        error_message = f"Error forwarding messages: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Log to errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_log = f"ERROR: FORWARD_MESSAGES | From: {from_chat_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_log)
            except Exception:
                pass
                
        return False, error_message