"""
Multi-Group Creator Module
Handles the creation of multiple groups using multiple sessions
"""

import asyncio
import datetime
import json
import time
import random
import logging
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserRestrictedError, ChatAdminRequiredError

from utils.floodwait import safe_execute
from utils.session_manager import check_session_valid
from modules.group_manager import create_supergroup, generate_invite_link
from utils.stats_manager import analyze_user_groups
import config

# Configure logger
logger = logging.getLogger(__name__)

# Store user sessions
USER_SESSIONS = {}
# Track created groups
CREATED_GROUPS = {}
# Store daily group creation count per session
DAILY_GROUP_COUNT = {}
# Store session errors
SESSION_ERRORS = {}
# Store random messages for posting in groups
RANDOM_MESSAGES = [
    "Hello everyone! Welcome to this group.",
    "This is an automated message for testing purposes.",
    "Thank you for joining this group.",
    "Hope you're having a great day!",
    "This group was created by a bot for demonstration.",
    "Feel free to chat here!",
    "Greetings from the automated system.",
    "This is message number {num} in the series.",
    "Automated message: The weather is great today!",
    "Technology makes amazing things possible."
]

async def handle_message(client, event):
    """
    Main handler for all messages
    
    Args:
        client: Telethon client
        event: Message event
    """
    # Skip outgoing messages (sent by the bot itself)
    if event.out:
        return
    
    # Get the message text
    if not event.message or not event.message.text:
        return
    
    text = event.message.text.strip()
    chat_id = event.chat_id
    
    # Check for commands
    if text == "/creategroups":
        await start_group_creation_flow(client, event)
        return
    
    # Check if this user has an active session
    if chat_id in USER_SESSIONS:
        session_data = USER_SESSIONS[chat_id]
        
        # Check the current stage and handle accordingly
        if session_data["stage"] == "ask_user_count":
            await handle_user_count_response(client, event, text)
            
        elif session_data["stage"] == "collect_sessions":
            await handle_session_input(client, event, text)
        
        elif session_data["stage"] == "ask_groups_per_session":
            await handle_groups_per_session_response(client, event, text)
            
        elif session_data["stage"] == "ask_group_prefix":
            await handle_group_prefix_response(client, event, text)
        
        elif session_data["stage"] == "ready_to_create" and text.lower() == "start":
            await start_group_creation(client, event)

async def start_group_creation_flow(client, event):
    """
    Start the group creation flow by asking for the number of users
    
    Args:
        client: Telethon client
        event: Message event
    """
    # Reset any existing sessions for this user
    if event.chat_id in USER_SESSIONS:
        del USER_SESSIONS[event.chat_id]
    
    # Initialize session storage for this chat
    USER_SESSIONS[event.chat_id] = {
        "stage": "ask_user_count",
        "sessions": [],
        "session_owners": {},
        "analyzed_sessions": [],
        "current_index": 0,
        "max_users": 0,
        "groups_per_session": config.GROUPS_PER_SESSION,
        "created_groups": [],
        "errors": {},
        "group_prefix": "Group",
        "total_available_groups": 0
    }
    
    # Ask for the number of users
    await event.respond("👥 **GROUP CREATION WIZARD** 👥\n\nHow many users do you want to have for making groups? Please enter a number.")

async def handle_user_count_response(client, event, text):
    """
    Handle the response for number of users
    
    Args:
        client: Telethon client
        event: Message event
        text: Message text (should be a number)
    """
    try:
        # Parse the number of users
        user_count = int(text.strip())
        
        if user_count <= 0:
            await event.respond("❌ Please enter a valid number greater than 0.")
            return
        
        # Update session data
        USER_SESSIONS[event.chat_id]["max_users"] = user_count
        USER_SESSIONS[event.chat_id]["stage"] = "collect_sessions"
        
        # Ask for the first session string
        await event.respond(f"📱 Please provide the string session for user 1 of {user_count}.\n\nRespond with the string session (it should start with '1A'... or similar).")
    
    except ValueError:
        await event.respond("❌ Invalid input. Please enter a valid number.")

async def handle_session_input(client, event, text):
    """
    Handle the session string input and analyze it immediately
    
    Args:
        client: Telethon client
        event: Message event
        text: Message text (should be a session string)
    """
    chat_id = event.chat_id
    session_data = USER_SESSIONS[chat_id]
    
    # Check if the input looks like a session string
    if not text.strip() or len(text.strip()) < 50:  # Most session strings are quite long
        await event.respond("❌ That doesn't look like a valid session string. Please provide a valid string session.")
        return
    
    try:
        # Show processing message
        processing_msg = await event.respond("🔄 Validating and analyzing session... Please wait...")
        
        # Validate the session string
        session_string = text.strip()
        is_valid = await check_session_valid(session_string, config.API_ID, config.API_HASH)
        
        if not is_valid:
            try:
                await processing_msg.delete()
            except Exception:
                pass  # Ignore deletion errors
            await event.respond("❌ Invalid session string. Please provide a valid string session.")
            return
        
        # Analyze the session to see how many groups it can create
        analysis = await analyze_user_groups(session_string, config.API_ID, config.API_HASH)
        
        if not analysis["success"]:
            try:
                await processing_msg.delete()
            except Exception:
                pass  # Ignore deletion errors
            await event.respond(f"⚠️ Warning: Could not analyze session. Will assume no groups created today.\nError: {analysis['error']}")
            analysis_data = {
                "user_id": 0,
                "username": "Unknown",
                "today_groups": 0,
                "available_groups": config.DAILY_GROUP_LIMIT,
                "total_groups": 0
            }
        else:
            analysis_data = analysis["data"]
        
        # Store the session and analysis data
        current_index = session_data["current_index"]
        
        # Add the session to our list
        session_data["sessions"].append(session_string)
        session_data["analyzed_sessions"].append(analysis_data)
        session_data["total_available_groups"] += analysis_data["available_groups"]
        
        # Create analysis results message
        analysis_message = (
            f"✅ Session {current_index + 1} accepted and analyzed!\n\n"
            f"**User:** {analysis_data['username']}\n"
            f"**Groups already created today:** {analysis_data['today_groups']}\n"
            f"**Groups available to create:** {analysis_data['available_groups']} of {config.DAILY_GROUP_LIMIT}"
        )
        
        # Try to edit the processing message, but handle errors gracefully
        try:
            await processing_msg.edit(analysis_message)
        except Exception as e:
            # If editing fails, delete the old message (if possible) and send a new one
            try:
                await processing_msg.delete()
            except Exception:
                pass  # Ignore deletion errors
            
            # Send a new message with the analysis results
            processing_msg = await event.respond(analysis_message)
        
        # Store the mapping of reply message ID to session index
        session_data["session_owners"][processing_msg.id] = current_index
        
        # Increment the index
        session_data["current_index"] += 1
        
        # Check if we've collected all sessions
        if session_data["current_index"] >= session_data["max_users"]:
            session_data["stage"] = "ask_groups_per_session"
            
            # Ask how many groups to create per session
            total_available = session_data["total_available_groups"]
            max_possible = session_data["max_users"] * config.DAILY_GROUP_LIMIT
            
            await event.respond(
                f"✅ All {session_data['max_users']} sessions collected and analyzed!\n\n"
                f"**Total groups available to create:** {total_available} of {max_possible}\n\n"
                f"How many groups do you want to create per session?\n"
                f"(Enter a number between 1 and {config.DAILY_GROUP_LIMIT}, default is {config.DAILY_GROUP_LIMIT})"
            )
        else:
            # Ask for the next session
            next_index = session_data["current_index"] + 1
            await event.respond(f"📱 Please provide the string session for user {next_index} of {session_data['max_users']}.")
    
    except Exception as e:
        await event.respond(f"❌ Error processing session: {str(e)}\nPlease try again with a valid session string.")
        logger.error(f"Error handling session input: {str(e)}")

async def handle_groups_per_session_response(client, event, text):
    """
    Handle the response for groups per session
    
    Args:
        client: Telethon client
        event: Message event
        text: Message text (should be a number)
    """
    chat_id = event.chat_id
    session_data = USER_SESSIONS[chat_id]
    
    try:
        # Parse the number of groups per session
        groups_per_session = int(text.strip())
        
        if groups_per_session <= 0:
            groups_per_session = config.DAILY_GROUP_LIMIT
        
        # Cap at the daily limit
        groups_per_session = min(groups_per_session, config.DAILY_GROUP_LIMIT)
        
        # Update session data
        session_data["groups_per_session"] = groups_per_session
        session_data["stage"] = "ask_group_prefix"
        
        # Ask for group prefix
        await event.respond(
            f"🏷️ What prefix would you like to use for group names?\n\n"
            f"This will be used to name groups as: [prefix]_[session]_[number]_[timestamp]\n"
            f"Default is 'Group'"
        )
    
    except ValueError:
        # If not a valid number, use the default
        session_data["groups_per_session"] = config.DAILY_GROUP_LIMIT
        session_data["stage"] = "ask_group_prefix"
        
        await event.respond(
            f"⚠️ Invalid input. Using default: {config.DAILY_GROUP_LIMIT} groups per session.\n\n"
            f"🏷️ What prefix would you like to use for group names?\n\n"
            f"This will be used to name groups as: [prefix]_[session]_[number]_[timestamp]\n"
            f"Default is 'Group'"
        )

async def handle_group_prefix_response(client, event, text):
    """
    Handle the response for group prefix
    
    Args:
        client: Telethon client
        event: Message event
        text: Message text (should be a prefix string)
    """
    chat_id = event.chat_id
    session_data = USER_SESSIONS[chat_id]
    
    # Get the prefix or use default
    prefix = text.strip()
    if not prefix:
        prefix = "Group"
    
    # Update session data
    session_data["group_prefix"] = prefix
    session_data["stage"] = "ready_to_create"
    
    # Final summary before creation
    total_groups = session_data["max_users"] * session_data["groups_per_session"]
    available_groups = sum(min(session_data["groups_per_session"], s["available_groups"]) for s in session_data["analyzed_sessions"])
    
    await event.respond(
        f"🚀 **READY TO CREATE GROUPS**\n\n"
        f"**Sessions:** {session_data['max_users']}\n"
        f"**Groups per session:** {session_data['groups_per_session']}\n"
        f"**Group name prefix:** {prefix}\n"
        f"**Maximum groups possible:** {total_groups}\n"
        f"**Available to create based on daily limits:** {available_groups}\n\n"
        f"Reply with 'start' to begin group creation."
    )

async def post_random_messages(client, group, count=10):
    """
    Post random messages in a group
    
    Args:
        client: Telethon client
        group: Group entity
        count: Number of messages to post (default increased to 10)
    """
    try:
        for i in range(count):
            # Select a random message
            message_template = random.choice(RANDOM_MESSAGES)
            # Format the message if it contains placeholders
            message = message_template.format(num=i+1) if "{num}" in message_template else message_template
            
            # Post the message to the group with error handling
            try:
                await safe_execute(client.send_message, group, message, max_attempts=2)
            except Exception as e:
                logger.error(f"Error posting message: {str(e)}")
                continue
            
            # Minimal delay between messages (just enough to avoid rate limits)
            await asyncio.sleep(0.3)
            
        return True
    except Exception as e:
        logger.error(f"Error posting messages: {str(e)}")
        return False

async def create_groups_for_session(client, event, session_string, session_idx, total_sessions, status_msg, analyzed_data):
    """
    Create multiple groups for a single session
    
    Args:
        client: Telethon bot client
        event: Message event
        session_string: User session string
        session_idx: Index of the current session
        total_sessions: Total number of sessions
        status_msg: Message to update with status
        analyzed_data: Analysis data for this session
    
    Returns:
        Tuple of (successful_groups, failed_groups)
    """
    chat_id = event.chat_id
    session_data = USER_SESSIONS[chat_id]
    successful_groups = []
    failed_groups = []
    error_count = 0
    
    try:
        # Create a client with this session
        session_client = TelegramClient(StringSession(session_string), config.API_ID, config.API_HASH)
        await session_client.connect()
        
        if not await session_client.is_user_authorized():
            error_msg = f"Session {session_idx + 1} is not authorized"
            failed_groups.append({"error": error_msg})
            session_data["errors"][f"session_{session_idx+1}"] = error_msg
            return [], [{"error": error_msg}]
        
        # Get the user's info for logging
        me = await session_client.get_me()
        user_name = f"{me.first_name} {me.last_name if me.last_name else ''}"
        user_id = me.id
        
        # Calculate today's date for tracking daily limits
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        session_key = f"{today}_{user_id}"
        
        # Initialize session tracking
        if session_key not in DAILY_GROUP_COUNT:
            DAILY_GROUP_COUNT[session_key] = 0
        
        # Use the analyzed data to determine how many groups to create
        available_groups = analyzed_data["available_groups"]
        
        # Create the configured number of groups for this session
        groups_to_create = min(
            session_data["groups_per_session"], 
            available_groups
        )
        
        if groups_to_create <= 0:
            error_msg = f"Daily limit reached for session {session_idx + 1}"
            failed_groups.append({"error": error_msg})
            session_data["errors"][f"session_{session_idx+1}"] = error_msg
            await event.respond(f"⚠️ Session {session_idx + 1} ({user_name}) has reached the daily group creation limit. Skipping...")
            return [], [{"error": error_msg}]
            
        await event.respond(
            f"🔄 Starting group creation for session {session_idx + 1} ({user_name})\n"
            f"Will create {groups_to_create} groups with this session"
        )
        
        # Create groups in batches with cooldowns
        for i in range(groups_to_create):
            try:
                # Check if we need a batch cooldown
                if i > 0 and i % config.BATCH_SIZE == 0:
                    batch_msg = f"✅ Created batch of {config.BATCH_SIZE} groups. Taking a short break to avoid rate limits..."
                    await event.respond(batch_msg)
                    await status_msg.edit(f"🔄 Group creation in progress...\n\nSession {session_idx + 1}/{total_sessions}\nCreating group {i+1}/{groups_to_create}\n\n{batch_msg}")
                    # Reduced cooldown for faster operation
                    await asyncio.sleep(config.BATCH_COOLDOWN / 2)
                
                # Generate unique group name
                timestamp = int(time.time())
                group_name = f"{session_data['group_prefix']}_{session_idx+1}_{i+1}_{timestamp}"
                
                # Update status message
                await status_msg.edit(
                    f"🔄 Group creation in progress...\n\n"
                    f"Session: {session_idx + 1}/{total_sessions}\n"
                    f"Creating group {i+1}/{groups_to_create}\n"
                    f"Total successful: {len(successful_groups)}\n"
                    f"Total failed: {len(failed_groups)}"
                )
                
                # Create the supergroup with retries
                success, group = await safe_execute(
                    create_supergroup, 
                    session_client, 
                    group_name, 
                    about=f"Supergroup created on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    max_attempts=config.MAX_RETRY_ATTEMPTS
                )
                
                if not success:
                    error_msg = f"Failed to create group: {group}"
                    failed_groups.append({
                        "group_index": i+1,
                        "error": error_msg
                    })
                    error_count += 1
                    
                    # If too many consecutive errors, break
                    if error_count >= config.MAX_ERRORS_PER_SESSION:
                        error_msg = f"Too many consecutive errors ({error_count}). Skipping remaining groups for this session."
                        session_data["errors"][f"session_{session_idx+1}"] = error_msg
                        await event.respond(f"⚠️ {error_msg}")
                        break
                        
                    continue
                
                # Reset error counter on success
                error_count = 0
                
                # Generate an invite link with retries
                invite_success, invite_link = await safe_execute(
                    generate_invite_link, 
                    session_client, 
                    group,
                    max_attempts=config.MAX_RETRY_ATTEMPTS
                )
                
                if not invite_success:
                    invite_link = "Not available"
                
                # Post 10 random messages in each group
                await post_random_messages(session_client, group, 10)
                
                # Add to created groups
                group_info = {
                    "group_id": group.id,
                    "title": group_name,
                    "session_index": session_idx,
                    "user_id": user_id,
                    "invite_link": invite_link,
                    "created_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                successful_groups.append(group_info)
                
                # Add to tracking
                if chat_id not in CREATED_GROUPS:
                    CREATED_GROUPS[chat_id] = []
                CREATED_GROUPS[chat_id].append(group_info)
                session_data["created_groups"].append(group_info)
                
                # Update daily count
                DAILY_GROUP_COUNT[session_key] = DAILY_GROUP_COUNT.get(session_key, 0) + 1
                
                # Only notify for every 5th group or if it's the last one for less spam
                if (i+1) % 5 == 0 or i+1 == groups_to_create:
                    await event.respond(
                        f"✅ Created groups {i-3 if i >= 4 else 1}-{i+1} for session {session_idx + 1}!\n"
                        f"Latest group: **{group_name}**\n"
                        f"Invite Link: {invite_link}\n\n"
                        f"Progress: {i+1}/{groups_to_create} groups for this session"
                    )
                
                # Reduced delay between group creations for faster operation
                if i < groups_to_create - 1:  # Don't delay after the last group
                    await asyncio.sleep(max(5, config.GROUP_CREATION_DELAY / 3))
            
            except FloodWaitError as e:
                # Handle FloodWaitError with explicit wait
                wait_time = e.seconds
                error_msg = f"FloodWait: Need to wait {wait_time} seconds"
                failed_groups.append({
                    "group_index": i+1, 
                    "error": error_msg
                })
                
                await event.respond(
                    f"⚠️ Rate limited by Telegram for session {session_idx + 1}.\n"
                    f"Need to wait {wait_time} seconds.\n"
                    f"Will continue after waiting..."
                )
                
                # Wait the required time
                await asyncio.sleep(wait_time)
                
            except UserRestrictedError:
                error_msg = "User is restricted from creating groups"
                failed_groups.append({
                    "group_index": i+1, 
                    "error": error_msg
                })
                session_data["errors"][f"session_{session_idx+1}"] = error_msg
                
                await event.respond(f"❌ Session {session_idx + 1} user is restricted. Skipping remaining groups for this session.")
                break
                
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                failed_groups.append({
                    "group_index": i+1, 
                    "error": error_msg
                })
                error_count += 1
                
                # Log the error
                if f"session_{session_idx+1}" not in session_data["errors"]:
                    session_data["errors"][f"session_{session_idx+1}"] = []
                if isinstance(session_data["errors"][f"session_{session_idx+1}"], list):
                    session_data["errors"][f"session_{session_idx+1}"].append(error_msg)
                else:
                    session_data["errors"][f"session_{session_idx+1}"] = [error_msg]
                
                # If too many consecutive errors, break
                if error_count >= config.MAX_ERRORS_PER_SESSION:
                    error_msg = f"Too many consecutive errors ({error_count}). Skipping remaining groups for this session."
                    await event.respond(f"⚠️ {error_msg}")
                    break
        
        # Disconnect the session client
        await session_client.disconnect()
        
        return successful_groups, failed_groups
        
    except Exception as e:
        error_msg = f"Session error: {str(e)}"
        session_data["errors"][f"session_{session_idx+1}"] = error_msg
        await event.respond(f"❌ Error with session {session_idx + 1}: {str(e)}")
        return successful_groups, failed_groups

async def start_group_creation(client, event):
    """
    Start the group creation process for multiple sessions
    
    Args:
        client: Telethon client
        event: Message event
    """
    chat_id = event.chat_id
    session_data = USER_SESSIONS[chat_id]
    
    # Initialize created groups tracking
    CREATED_GROUPS[chat_id] = []
    
    # Start status message
    status_msg = await event.respond("🔄 Starting group creation process...\n\n0 groups created so far.")
    
    # Track overall statistics
    total_successful = 0
    total_failed = 0
    all_issues = {}
    
    # Process each session
    for idx, session_string in enumerate(session_data["sessions"]):
        try:
            analyzed_data = session_data["analyzed_sessions"][idx]
            
            # Update status message
            await status_msg.edit(
                f"🔄 Processing session {idx + 1} of {session_data['max_users']}...\n\n"
                f"Total groups created so far: {total_successful}"
            )
            
            # Create groups for this session
            await event.respond(f"🔄 Starting group creation with session {idx + 1}...")
            successful, failed = await create_groups_for_session(
                client, 
                event, 
                session_string, 
                idx, 
                session_data["max_users"],
                status_msg,
                analyzed_data
            )
            
            # Update statistics
            total_successful += len(successful)
            total_failed += len(failed)
            
            # Track issues
            if failed:
                all_issues[f"session_{idx+1}"] = failed
            
            # Session summary
            await event.respond(
                f"✅ Finished processing session {idx + 1}:\n\n"
                f"• Successfully created: {len(successful)} groups\n"
                f"• Failed: {len(failed)} groups\n\n"
                f"Moving to next session..."
            )
            
            # Add delay between sessions (reduced for faster operation)
            if idx < len(session_data["sessions"]) - 1:
                await event.respond(f"⏳ Taking a short break before switching to the next session...")
                await asyncio.sleep(config.SESSION_SWITCH_DELAY / 2)
        
        except Exception as e:
            await event.respond(f"❌ Unexpected error with session {idx + 1}: {str(e)}")
            all_issues[f"session_{idx+1}_critical"] = str(e)
    
    # Final summary
    total_attempted = total_successful + total_failed
    success_rate = (total_successful / total_attempted) * 100 if total_attempted > 0 else 0
    
    # Generate issues summary
    issues_summary = ""
    if all_issues:
        issues_summary = "\n\n🔍 **Issues encountered:**\n"
        for session_key, issues in all_issues.items():
            issues_summary += f"\n• {session_key}: "
            if isinstance(issues, list):
                if len(issues) > 3:
                    issues_summary += f"{len(issues)} issues - sample: {issues[0]['error']}, {issues[1]['error']}, {issues[2]['error']}..."
                else:
                    issues_summary += f"{len(issues)} issues - {', '.join([i['error'] for i in issues])}"
            else:
                issues_summary += f"{issues}"
    
    # Final status message
    await status_msg.edit(
        f"✅ Group creation completed!\n\n"
        f"**Total groups created:** {total_successful}/{total_attempted} ({success_rate:.1f}%)\n"
        f"**Sessions processed:** {session_data['max_users']}\n"
        f"**Groups per session:** {session_data['groups_per_session']}\n"
        f"**Group prefix used:** {session_data['group_prefix']}\n"
        f"**Date:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"{issues_summary}"
    )
    
    # Generate detailed report
    report = (
        f"📊 **DETAILED GROUP CREATION REPORT**\n\n"
        f"**Total groups created:** {total_successful}\n"
        f"**Failed creation attempts:** {total_failed}\n"
        f"**Success rate:** {success_rate:.1f}%\n"
        f"**Sessions used:** {session_data['max_users']}\n\n"
    )
    
    # Add session-by-session breakdown
    for idx in range(session_data["max_users"]):
        session_groups = [g for g in CREATED_GROUPS[chat_id] if g["session_index"] == idx]
        report += f"**Session {idx+1}:** {len(session_groups)} groups created\n"
    
    # Send the detailed report
    await event.respond(report)
    
    # Clear session data to free memory
    if chat_id in USER_SESSIONS:
        # Keep only essential statistics
        USER_SESSIONS[chat_id] = {
            "completed": True,
            "total_created": total_successful,
            "total_failed": total_failed,
            "errors": session_data["errors"]
        }