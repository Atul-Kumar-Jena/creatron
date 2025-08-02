"""
Multi-Commands Module
Handles various commands for the Telegram bot
"""

import asyncio
import datetime
import re
import json
import logging
from telethon import events, Button
from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
from telethon.tl.types import InputPeerUser

import config
from modules.multi_group_creator import start_group_creation_flow, handle_message
from utils.session_manager import store_session, load_sessions
from utils.stats_manager import analyze_user_groups, get_group_creation_summary

# Configure logger
logger = logging.getLogger(__name__)

# Global session storage
USER_SESSIONS = {}
ACTIVE_SESSIONS = {}
ANALYZING_SESSIONS = {}

async def load_sessions_from_storage_group(client):
    """Load user sessions from the storage group"""
    try:
        # Check if storage group exists
        try:
            entity = await client.get_entity(config.SESSION_STORAGE_GROUP_ID)
            logger.info(f"Found storage group: {entity.title if hasattr(entity, 'title') else entity.id}")
        except:
            logger.error("Could not find storage group")
            return
        
        # Load existing sessions
        sessions = await load_sessions(client, config.SESSION_STORAGE_GROUP_ID)
        
        if not sessions:
            logger.info("No sessions found in storage group")
            return
        
        logger.info(f"Loaded {len(sessions)} sessions from storage")
        
        # Store the sessions
        for user_id, user_sessions in sessions.items():
            USER_SESSIONS[user_id] = user_sessions
            logger.info(f"Loaded {len(user_sessions)} sessions for user {user_id}")
            
    except Exception as e:
        logger.error(f"Error loading sessions: {str(e)}")

async def handle_message(client, event):
    """Handle all messages received by the bot"""
    # Skip events that aren't text messages
    if not event.message or not hasattr(event.message, 'text'):
        return
        
    # Handle command messages
    if await handle_command_message(client, event):
        return
    
    # Check for group creation flow first - this should take priority
    from modules.multi_group_creator import USER_SESSIONS as GROUP_CREATION_SESSIONS
    chat_id = event.chat_id
    
    # If this chat is in a group creation flow, always handle that first
    if chat_id in GROUP_CREATION_SESSIONS and "stage" in GROUP_CREATION_SESSIONS[chat_id]:
        # Pass to multi-group creator handler
        from modules.multi_group_creator import handle_message as mgc_handle_message
        await mgc_handle_message(client, event)
        return
        
    # Now handle analyzing sessions if not in group creation flow
    if event.chat_id in ANALYZING_SESSIONS and ANALYZING_SESSIONS[event.chat_id]:
        await handle_analyze_session_input(client, event)
        return
        
    # Otherwise pass to the normal multi-group creator handler
    from modules.multi_group_creator import handle_message as mgc_handle_message
    await mgc_handle_message(client, event)

async def handle_command_message(client, event):
    """Handle bot command messages"""
    text = event.message.text.strip().lower()
    chat_id = event.chat_id
    user_id = event.sender_id
    
    # Check for bot commands
    if text.startswith('/'):
        command = text.split(' ')[0]
        
        if command == '/start':
            await handle_start_command(client, event)
            return True
            
        elif command == '/creategroups':
            await start_group_creation_flow(client, event)
            return True
            
        elif command == '/help':
            await handle_help_command(client, event)
            return True
            
        elif command == '/summary':
            await handle_summary_command(client, event)
            return True
            
        elif command == '/analyze':
            await handle_analyze_command(client, event)
            return True
            
        elif command == '/status':
            await handle_status_command(client, event)
            return True

    return False

async def handle_start_command(client, event):
    """Handle /start command"""
    await event.respond(
        "👋 **Welcome to the Multi-Group Creator Bot!**\n\n"
        "This bot allows you to create multiple Telegram supergroups using multiple user accounts.\n\n"
        "**Available commands:**\n"
        "/creategroups - Start the group creation wizard\n"
        "/analyze - Analyze groups created by a session\n"
        "/summary - View your group creation summary\n"
        "/status - Check the bot's status\n"
        "/help - Show this help message\n\n"
        "To get started, use the /creategroups command."
    )

async def handle_help_command(client, event):
    """Handle /help command"""
    await event.respond(
        "📚 **Multi-Group Creator Bot Help**\n\n"
        "This bot helps you create multiple Telegram supergroups using your user accounts.\n\n"
        "**Command List:**\n\n"
        "🔹 /start - Start the bot\n"
        "🔹 /creategroups - Start the group creation wizard\n"
        "🔹 /analyze - Check how many groups you can still create with a session\n"
        "🔹 /summary - View your group creation summary\n"
        "🔹 /status - Check the bot's status\n"
        "🔹 /help - Show this help message\n\n"
        "**How to use:**\n"
        "1. Use /creategroups to start the wizard\n"
        "2. Follow the prompts to provide user sessions\n"
        "3. The bot will create groups using those sessions\n"
        "4. Use /summary to view results\n\n"
        "For more information, contact the bot administrator."
    )

async def handle_summary_command(client, event):
    """Handle /summary command"""
    chat_id = event.chat_id
    
    # Get summary data
    summary_result = await get_group_creation_summary(chat_id)
    
    if not summary_result["success"]:
        await event.respond(f"❌ {summary_result['error']}")
        return
    
    summary = summary_result["data"]
    
    # Format the summary message
    response = (
        f"📊 **GROUP CREATION SUMMARY**\n\n"
        f"**Total sessions used:** {summary['total_sessions']}\n"
        f"**Total groups created:** {summary['total_groups']}\n\n"
    )
    
    # Add session breakdown
    if summary["groups_by_session"]:
        response += "**Groups by session:**\n"
        for session_idx, count in summary["groups_by_session"].items():
            response += f"Session {int(session_idx)+1}: {count} groups\n"
        response += "\n"
    
    # Add date breakdown
    if summary["groups_by_date"]:
        response += "**Groups by date:**\n"
        sorted_dates = sorted(summary["groups_by_date"].keys(), reverse=True)
        for date in sorted_dates:
            count = summary["groups_by_date"][date]
            response += f"{date}: {count} groups\n"
    
    await event.respond(response)

async def handle_analyze_command(client, event):
    """Handle /analyze command to analyze groups created with a session"""
    chat_id = event.chat_id
    
    # Set the chat to analyzing mode
    ANALYZING_SESSIONS[chat_id] = True
    
    await event.respond(
        "📊 **ANALYZE SESSION GROUPS**\n\n"
        "This command will check how many groups have been created today using a particular session, "
        "and how many more can be created within the daily limit.\n\n"
        "Please send the string session you want to analyze.\n"
        "(It should start with '1A'... or similar)"
    )

async def handle_analyze_session_input(client, event):
    """Handle session string input for analysis"""
    chat_id = event.chat_id
    text = event.message.text.strip()
    
    # Validate session string format (simple check)
    if len(text) < 50:
        await event.respond("❌ That doesn't look like a valid session string. Please try again or use /cancel to cancel.")
        return
    
    # Clear analyzing mode
    ANALYZING_SESSIONS[chat_id] = False
    
    # Show processing message
    processing_msg = await event.respond("🔄 Analyzing session... Please wait, this may take a minute...")
    
    # Analyze the session
    analysis = await analyze_user_groups(text, config.API_ID, config.API_HASH)
    
    # Delete processing message
    await processing_msg.delete()
    
    if not analysis["success"]:
        await event.respond(f"❌ Error analyzing session: {analysis['error']}")
        return
    
    stats = analysis["data"]
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Compose response message
    response = (
        f"📊 **SESSION ANALYSIS RESULTS**\n\n"
        f"**User:** {stats['username']}\n"
        f"**User ID:** {stats['user_id']}\n\n"
        f"**Groups Created Today:** {stats['today_groups']} groups\n"
        f"**Daily Group Limit:** {config.DAILY_GROUP_LIMIT} groups\n"
        f"**Available Today:** {stats['available_groups']} more groups can be created\n\n"
        f"**Total Groups Created:** {stats['total_groups']} groups\n"
    )
    
    # Add groups by date if available
    if stats["groups_by_date"]:
        response += "\n**Groups Created By Date:**\n"
        sorted_dates = sorted(stats["groups_by_date"].keys(), reverse=True)
        for date in sorted_dates[:7]:  # Show last 7 days
            count = stats["groups_by_date"][date]
            if date == today:
                response += f"• {date}: {count} groups (Today)\n"
            else:
                response += f"• {date}: {count} groups\n"
    
    await event.respond(response)

async def handle_status_command(client, event):
    """Handle /status command"""
    from modules.multi_group_creator import USER_SESSIONS, DAILY_GROUP_COUNT, CREATED_GROUPS
    
    # Get today's date for stats
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Calculate total groups created today across all sessions
    today_total = sum([count for key, count in DAILY_GROUP_COUNT.items() if key.startswith(today)])
    
    # Format response
    response = (
        f"🤖 **BOT STATUS**\n\n"
        f"**Date:** {today}\n"
        f"**API Credentials:** {'✅ Valid' if config.API_ID and config.API_HASH else '❌ Missing'}\n"
        f"**Active Sessions:** {len(USER_SESSIONS)}\n"
        f"**Groups Created Today:** {today_total}\n"
        f"**Daily Group Limit:** {config.DAILY_GROUP_LIMIT} per session\n"
        f"**Groups Per Session:** {config.GROUPS_PER_SESSION}\n\n"
        f"Bot is running normally and ready to create groups."
    )
    
    await event.respond(response)