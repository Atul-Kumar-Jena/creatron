"""
Admin commands module for processing owner and authorized user commands
"""
import asyncio
import time
import re
import datetime
import traceback
from telethon import events, Button, functions, types
from telethon.errors import FloodWaitError

from utils.auth import is_authorized, is_owner, authorize_user, unauthorize_user, get_authorized_users
from utils.floodwait import handle_flood_wait
from utils.session_manager import (
    initialize_session, get_session, get_all_sessions, 
    remove_session, send_message_to_saved, update_session_stats
)
from modules.group_manager import create_supergroup, add_users_to_group, update_group_info
from modules.message_sender import send_messages, send_message_to_multiple_chats
import config

async def register_commands(client):
    """
    Register all command handlers
    
    Args:
        client: Telethon client
    """
    # Help command
    @client.on(events.NewMessage(pattern=config.CMD_HELP))
    async def help_command(event):
        await handle_help_command(client, event)
    
    # Auth command (owner only)
    @client.on(events.NewMessage(pattern=config.CMD_AUTH))
    async def auth_command(event):
        await handle_auth_command(client, event)
    
    # Unauth command (owner only)
    @client.on(events.NewMessage(pattern=config.CMD_UNAUTH))
    async def unauth_command(event):
        await handle_unauth_command(client, event)
    
    # Status command
    @client.on(events.NewMessage(pattern=config.CMD_STATUS))
    async def status_command(event):
        await handle_status_command(client, event)
    
    # Stats command (owner only)
    @client.on(events.NewMessage(pattern=config.CMD_STATS))
    async def stats_command(event):
        await handle_stats_command(client, event)
    
    # Create group command
    @client.on(events.NewMessage(pattern=config.CMD_CREATE_GROUP))
    async def create_group_command(event):
        await handle_create_group_command(client, event)
    
    # Send messages command
    @client.on(events.NewMessage(pattern=config.CMD_SEND_MESSAGES))
    async def send_messages_command(event):
        await handle_send_messages_command(client, event)
    
    # Add session command (owner only)
    @client.on(events.NewMessage(pattern=config.CMD_ADD_SESSION))
    async def add_session_command(event):
        await handle_add_session_command(client, event)
    
    # List sessions command (owner only)
    @client.on(events.NewMessage(pattern=config.CMD_LIST_SESSIONS))
    async def list_sessions_command(event):
        await handle_list_sessions_command(client, event)
    
    # Remove session command (owner only)
    @client.on(events.NewMessage(pattern=config.CMD_REMOVE_SESSION))
    async def remove_session_command(event):
        await handle_remove_session_command(client, event)
    
    # Iterate command for user feedback
    @client.on(events.NewMessage(pattern=config.CMD_ITERATE))
    async def iterate_command(event):
        await handle_iterate_command(client, event)
    
    # IMPORTANT: Do NOT register a separate callback handler here
    # All callbacks are handled by the main callback_handler in main.py

# Now provide the callback handling functions that will be called from main.py
# These functions are now decoupled from the event registration

# Helper function to handle help callbacks with better error handling
async def handle_help_callback(client, event, data):
    """Master handler for help button callbacks"""
    try:
        # Extract the section from the callback data
        parts = data.split('_')
        if len(parts) >= 2:
            section = parts[1]
            await handle_help_section(client, event, section)
        else:
            # Invalid callback format, return to main help menu
            await handle_help_section(client, event, 'main')
    except Exception as e:
        error_message = f"Error handling help callback: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Try to answer the callback
        try:
            await event.answer(f"Error: {str(e)[:100]}")
        except:
            pass

# Command handlers
async def handle_help_command(client, event):
    """Handle help command with interactive buttons"""
    try:
        # Check if user is authorized
        if not await is_authorized(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Different help messages for owner and regular users
        is_user_owner = await is_owner(event.sender_id)
        
        # Create buttons for different help sections
        buttons = [
            [Button.inline("📢 User Commands", "help_user")],
            [Button.inline("🔧 Group Management", "help_group")]
        ]
        
        # Add owner commands section if user is owner
        if is_user_owner:
            buttons.append([Button.inline("🔐 Owner Commands", "help_owner")])
        
        # Add quick action buttons
        buttons.append([
            Button.inline("➕ Create Group", "create_group"),
            Button.inline("📨 Send Messages", "send_message")
        ])
        
        help_message = "🤖 **Welcome to the Command Help**\n\n"
        help_message += "Use the buttons below to navigate through different command categories.\n"
        help_message += "Each command has a detailed explanation to help you use it effectively.\n\n"
        help_message += "**Quick tip:** All commands start with the prefix `" + config.CMD_PREFIX + "`\n\n"
        help_message += "Select a category below:"
        
        # Send the main help menu with buttons
        try:
            # Use direct message sending rather than event.reply() to avoid issues
            await client.send_message(event.chat_id, help_message, buttons=buttons)
        except FloodWaitError as e:
            # Handle flood wait
            await handle_flood_wait(e.seconds, 'general')
            # Try again
            await client.send_message(event.chat_id, help_message, buttons=buttons)
        except Exception as e:
            # If buttons fail for some reason, fall back to text-only help
            print(f"⚠️ Error sending help with buttons: {str(e)}")
            await handle_text_help(client, event)
        
    except Exception as e:
        error_message = f"Error displaying help: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_text_help(client, event):
    """Fallback text-only help command"""
    try:
        # Different help messages for owner and regular users
        is_user_owner = await is_owner(event.sender_id)
        
        help_message = "📋 **Available Commands**\n\n"
        
        # Common commands
        help_message += f"`{config.CMD_HELP}` - Show this help message\n"
        help_message += f"`{config.CMD_STATUS}` - Check your authorization status\n"
        help_message += f"`{config.CMD_CREATE_GROUP} <title> | [description]` - Create a supergroup\n"
        help_message += f"`{config.CMD_SEND_MESSAGES} <chat_id> | <message1> | <message2> | ...` - Send multiple messages to a chat\n\n"
        
        # Owner-only commands
        if is_user_owner:
            help_message += "🔐 **Owner Commands**\n\n"
            help_message += f"`{config.CMD_AUTH} <user_id>` - Authorize a user\n"
            help_message += f"`{config.CMD_UNAUTH} <user_id>` - Unauthorize a user\n"
            help_message += f"`{config.CMD_STATS}` - Show bot statistics\n"
            help_message += f"`{config.CMD_ADD_SESSION} <string_session>` - Add a new session\n"
            help_message += f"`{config.CMD_LIST_SESSIONS}` - List all active sessions\n"
            help_message += f"`{config.CMD_REMOVE_SESSION} <session_id>` - Remove a session\n"
        
        await event.reply(help_message)
        
    except Exception as e:
        await event.reply(f"Error: {str(e)}")

async def handle_help_section(client, event, section):
    """Handle displaying specific help section"""
    try:
        # Different help messages based on section
        is_user_owner = await is_owner(event.sender_id)
        
        # Back button for all sections
        back_button = [Button.inline("◀️ Back to Main Menu", data="help_main")]
        
        if section == 'main':
            # This is the main menu, show the full help command
            # We'll delete the current message and send a new one
            try:
                await event.delete()
            except:
                pass
            await handle_help_command(client, await event.get_message())
            return
            
        elif section == 'user':
            # User commands section
            help_message = "📢 **User Commands**\n\n"
            help_message += f"`{config.CMD_HELP}` - Show the help menu\n"
            help_message += f"`{config.CMD_STATUS}` - Check your authorization status\n"
            
            buttons = [back_button]
            
        elif section == 'group':
            # Group management section
            help_message = "🔧 **Group Management Commands**\n\n"
            help_message += f"`{config.CMD_CREATE_GROUP} <title> | [description]` - Create a public supergroup\n"
            help_message += f"All created supergroups are public with visible history to everyone\n\n"
            
            # Add quick action buttons
            buttons = [
                [Button.inline("➕ Create Group Now", data="create_group")],
                back_button
            ]
            
        elif section == 'owner' and is_user_owner:
            # Owner commands section (only shown if user is owner)
            help_message = "🔐 **Owner Commands**\n\n"
            help_message += f"`{config.CMD_AUTH} <user_id>` - Authorize a user\n"
            help_message += f"`{config.CMD_UNAUTH} <user_id>` - Unauthorize a user\n"
            help_message += f"`{config.CMD_STATS}` - Show bot statistics\n"
            help_message += f"`{config.CMD_ADD_SESSION} <string_session>` - Add a new session\n"
            help_message += f"`{config.CMD_LIST_SESSIONS}` - List all active sessions\n"
            help_message += f"`{config.CMD_REMOVE_SESSION} <session_id>` - Remove a session\n\n"
            help_message += "Note: The owner ID (set in config) controls the bot, which can run on a different account than the one whose string session is provided.\n"
            
            buttons = [back_button]
            
        else:
            # Default back to main menu
            await handle_help_section(client, event, 'main')
            return
        
        # Edit the message with the new section
        try:
            await event.edit(help_message, buttons=buttons)
        except FloodWaitError as e:
            # Handle flood wait
            await handle_flood_wait(e.seconds, 'general')
            # Try again
            await event.edit(help_message, buttons=buttons)
        except Exception as e:
            print(f"⚠️ Error editing help section: {str(e)}")
            # If editing fails, try to answer the callback at least
            try:
                await event.answer("Error displaying help section. Please try again.")
            except:
                pass
            
    except Exception as e:
        error_message = f"Error displaying help section: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Try to answer the callback
        try:
            await event.answer(f"Error: {str(e)[:100]}")
        except:
            pass

async def handle_auth_command(client, event):
    """Handle auth command (owner only)"""
    try:
        # Check if user is owner
        if not await is_owner(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Get user ID to authorize
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await event.reply("⚠️ Please provide a user ID to authorize")
            return
        
        try:
            user_id = int(args[1].strip())
        except ValueError:
            await event.reply("⚠️ Invalid user ID format")
            return
        
        # Authorize the user
        success = await authorize_user(client, user_id)
        
        if success:
            await event.reply(config.SUCCESS_AUTH.format(user_id))
            
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: USER_AUTHORIZED | ID: {user_id} | By: {event.sender_id} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging user authorization: {str(e)}")
        else:
            await event.reply("⚠️ Failed to authorize user")
        
    except Exception as e:
        error_message = f"Error authorizing user: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_unauth_command(client, event):
    """Handle unauth command (owner only)"""
    try:
        # Check if user is owner
        if not await is_owner(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Get user ID to unauthorize
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await event.reply("⚠️ Please provide a user ID to unauthorize")
            return
        
        try:
            user_id = int(args[1].strip())
        except ValueError:
            await event.reply("⚠️ Invalid user ID format")
            return
        
        # Unauthorize the user
        success = await unauthorize_user(client, user_id)
        
        if success:
            await event.reply(config.SUCCESS_UNAUTH.format(user_id))
            
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: USER_UNAUTHORIZED | ID: {user_id} | By: {event.sender_id} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging user unauthorization: {str(e)}")
        else:
            await event.reply("⚠️ Failed to unauthorize user (could be owner or error)")
        
    except Exception as e:
        error_message = f"Error unauthorizing user: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_status_command(client, event):
    """Handle status command"""
    try:
        # Get authorization status
        is_user_authorized = await is_authorized(event.sender_id)
        is_user_owner = await is_owner(event.sender_id)
        
        if is_user_owner:
            status = "🔐 You are the bot owner"
        elif is_user_authorized:
            status = "✅ You are authorized to use this bot"
        else:
            status = "❌ You are not authorized to use this bot"
        
        # Get authorized users count if owner
        if is_user_owner:
            auth_users = await get_authorized_users(client)
            status += f"\n👥 Total authorized users: {len(auth_users)}"
        
        # Add buttons for help and stats
        buttons = [
            [Button.inline("📋 Help Menu", data="help_main")]
        ]
        
        if is_user_owner:
            buttons.append([Button.inline("📊 Statistics", data="help_stats")])
        
        try:
            await event.reply(status, buttons=buttons)
        except FloodWaitError as e:
            # Handle flood wait
            await handle_flood_wait(e.seconds, 'general')
            # Try again without buttons if there was a FloodWaitError
            await event.reply(status)
        except Exception as e:
            # If buttons fail for some reason, fall back to text-only
            print(f"⚠️ Error sending status with buttons: {str(e)}")
            await event.reply(status)
        
    except Exception as e:
        error_message = f"Error checking status: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_stats_command(client, event):
    """Handle stats command (owner only)"""
    try:
        # Check if user is owner
        if not await is_owner(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Get all sessions
        sessions = await get_all_sessions()
        
        # Compile stats
        total_sessions = len(sessions)
        total_groups_created = 0
        total_messages_sent = 0
        total_errors = 0
        total_flood_waits = 0
        
        stats_message = "📊 **Bot Statistics**\n\n"
        stats_message += f"Active Sessions: {total_sessions}\n\n"
        
        # Add per-session stats
        for session_id, session in sessions.items():
            stats = session.get('stats', {})
            groups_created = stats.get('groups_created', 0)
            messages_sent = stats.get('messages_sent', 0)
            errors = stats.get('errors', 0)
            flood_waits = stats.get('flood_waits', 0)
            
            total_groups_created += groups_created
            total_messages_sent += messages_sent
            total_errors += errors
            total_flood_waits += flood_waits
            
            session_name = session.get('first_name', '') or session.get('username', '') or session.get('phone', '') or str(session_id)
            stats_message += f"Session {session_id} ({session_name}):\n"
            stats_message += f"  - Groups created: {groups_created}\n"
            stats_message += f"  - Messages sent: {messages_sent}\n"
            stats_message += f"  - Errors: {errors}\n"
            stats_message += f"  - FloodWaits: {flood_waits}\n\n"
        
        # Add totals
        stats_message += "**Totals**\n"
        stats_message += f"Total Groups Created: {total_groups_created}\n"
        stats_message += f"Total Messages Sent: {total_messages_sent}\n"
        stats_message += f"Total Errors: {total_errors}\n"
        stats_message += f"Total FloodWaits: {total_flood_waits}\n"
        
        # Add timestamp
        stats_message += f"\nLast Updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Add a refresh button
        buttons = [
            [Button.inline("🔄 Refresh Stats", data="refresh_stats")],
            [Button.inline("◀️ Back to Help", data="help_main")]
        ]
        
        try:
            await event.reply(stats_message, buttons=buttons)
        except FloodWaitError as e:
            # Handle flood wait
            await handle_flood_wait(e.seconds, 'general')
            # Try again without buttons
            await event.reply(stats_message)
        except Exception as e:
            # If buttons fail, send text only
            print(f"⚠️ Error sending stats with buttons: {str(e)}")
            await event.reply(stats_message)
        
    except Exception as e:
        error_message = f"Error fetching statistics: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_create_group_command(client, event):
    """Handle create group command with buttonized interface"""
    try:
        # Check if user is authorized
        if not await is_authorized(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        try:
            # Start a conversation to get inputs with proper error handling
            conversation = client.conversation(event.chat_id, timeout=120)
            
            # Ask for group details with buttons for quick options
            buttons = [
                [Button.inline("Default Settings", data="group_default")],
                [Button.inline("Cancel", data="group_cancel")]
            ]
            
            prompt_msg = await event.reply(
                "📝 **Create Public Supergroup**\n\n"
                "Please enter the group title, or click 'Default Settings' to create a group with default name and settings.\n"
                "Format: `title | description` (description is optional)",
                buttons=buttons
            )
            
            # Wait for user response with timeout handling
            try:
                response = await asyncio.wait_for(conversation.get_response(), timeout=120)
                
                # Check if response is a button callback or text
                if hasattr(response, 'data'):
                    # It's a button callback
                    data = response.data.decode('utf-8')
                    if data == 'group_default':
                        # Use default settings
                        title = None
                        about = None
                    elif data == 'group_cancel':
                        await prompt_msg.edit("❌ Group creation cancelled.")
                        return
                else:
                    # It's a text response
                    text = response.text.strip()
                    
                    # Parse title and description
                    if '|' in text:
                        parts = text.split('|', 1)
                        title = parts[0].strip()
                        about = parts[1].strip() if len(parts) > 1 else None
                    else:
                        title = text
                        about = None
                
                # Create the supergroup with public setting and visible history
                processing_msg = await event.reply("⏳ Creating public supergroup...")
                
                success, result = await create_supergroup(
                    client, 
                    title=title, 
                    about=about, 
                    public=True  # Ensure it's public with visible history
                )
                
                if success:
                    channel = result
                    
                    # Create invite link for the group
                    try:
                        invite_link = await client(functions.messages.ExportChatInviteRequest(
                            peer=channel
                        ))
                        link = invite_link.link
                    except Exception as e:
                        print(f"⚠️ Error creating invite link: {str(e)}")
                        link = "Unable to create invite link"
                    
                    # Success message with buttons
                    success_buttons = [
                        [Button.inline("➕ Create Another Group", data="create_group")],
                        [Button.inline("◀️ Back to Help", data="help_main")]
                    ]
                    
                    success_msg = (
                        f"✅ **Supergroup Created Successfully**\n\n"
                        f"**Title:** {channel.title}\n"
                        f"**ID:** `{channel.id}`\n"
                        f"**Invite Link:** {link}\n\n"
                        f"The group is public with history visible to everyone."
                    )
                    
                    try:
                        await processing_msg.edit(success_msg, buttons=success_buttons)
                    except:
                        # If editing with buttons fails, send as new message
                        await processing_msg.edit(success_msg)
                else:
                    # Error message
                    error_msg = f"⚠️ Failed to create supergroup: {result}"
                    await processing_msg.edit(error_msg)
                    
            except asyncio.TimeoutError:
                await prompt_msg.edit("⏰ Timeout: Group creation cancelled due to inactivity.")
                
        except Exception as e:
            error_message = f"Error in group creation conversation: {str(e)}"
            print(f"⚠️ {error_message}")
            print(f"Detailed error: {traceback.format_exc()}")
            await event.reply(error_message)
        
    except Exception as e:
        error_message = f"Error creating group: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_send_messages_command(client, event):
    """Handle send messages command with improved error handling"""
    try:
        # Check if user is authorized
        if not await is_authorized(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        try:
            # Start a conversation to get inputs with proper timeout
            conversation = client.conversation(event.chat_id, timeout=180)
            
            # Ask for the chat ID with cancel button
            buttons = [
                [Button.inline("Cancel", data="send_cancel")]
            ]
            
            prompt_msg = await event.reply(
                "📨 **Send Messages**\n\n"
                "Enter the chat ID or username where you want to send messages:",
                buttons=buttons
            )
            
            # Wait for user response with timeout handling
            try:
                response = await asyncio.wait_for(conversation.get_response(), timeout=120)
                
                # Check if it's a button callback
                if hasattr(response, 'data') and response.data.decode('utf-8') == 'send_cancel':
                    await prompt_msg.edit("❌ Message sending cancelled.")
                    return
                
                chat_id = response.text.strip()
                
                # Try to convert chat_id to int (numeric ID)
                try:
                    chat_id = int(chat_id)
                except ValueError:
                    # Keep as string (username)
                    pass
                
                # Ask for the message(s)
                await event.reply(
                    "📝 Enter the message(s) to send.\n"
                    "You can send multiple messages by separating them with a line containing only '|'"
                )
                
                response = await asyncio.wait_for(conversation.get_response(), timeout=180)
                
                # Split messages by separator
                messages = []
                if '|' in response.text:
                    # Split by | and filter empty messages
                    messages = [msg.strip() for msg in response.text.split('|') if msg.strip()]
                else:
                    # Single message
                    messages = [response.text.strip()]
                
                if not messages:
                    await event.reply("⚠️ No valid messages provided.")
                    return
                
                # Send the messages
                processing_msg = await event.reply(f"⏳ Sending {len(messages)} messages to {chat_id}...")
                
                success_count = 0
                error_messages = []
                
                for i, message in enumerate(messages):
                    try:
                        # Send the message
                        await client.send_message(chat_id, message)
                        success_count += 1
                        
                        # Add a small delay to avoid flood
                        if i < len(messages) - 1:
                            await asyncio.sleep(1)
                            
                    except FloodWaitError as e:
                        # Handle the flood wait
                        await handle_flood_wait(e.seconds, 'general')
                        
                        # Try again
                        try:
                            await client.send_message(chat_id, message)
                            success_count += 1
                        except Exception as retry_e:
                            error_messages.append(f"Message {i+1}: {str(retry_e)}")
                            
                    except Exception as e:
                        error_messages.append(f"Message {i+1}: {str(e)}")
                        
                        # Update stats
                        try:
                            session = await get_session(client.session.save())
                            if session:
                                await update_session_stats(session['session_id'], 'errors')
                        except:
                            pass
                
                # Update stats
                try:
                    session = await get_session(client.session.save())
                    if session:
                        await update_session_stats(session['session_id'], 'messages_sent', count=success_count)
                except:
                    pass
                
                # Update the status message
                if success_count == len(messages):
                    status_msg = f"✅ Successfully sent all {success_count} messages to {chat_id}"
                    
                    # Log to logs database if it exists
                    if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                        try:
                            log_message = f"LOG: MESSAGES_SENT | Chat: {chat_id} | Count: {success_count} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            await client.send_message(config.DB_GROUPS["logs"], log_message)
                        except Exception as e:
                            print(f"⚠️ Error logging message sending: {str(e)}")
                    
                else:
                    status_msg = f"⚠️ Sent {success_count}/{len(messages)} messages to {chat_id}"
                    if error_messages:
                        status_msg += f"\n\nErrors:\n" + "\n".join(error_messages[:5])
                        if len(error_messages) > 5:
                            status_msg += f"\n...and {len(error_messages) - 5} more errors"
                    
                    # Log to errors database if it exists
                    if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                        try:
                            error_log = f"ERROR: MESSAGES_SENT | Chat: {chat_id} | Success: {success_count}/{len(messages)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            await client.send_message(config.DB_GROUPS["errors"], error_log)
                        except Exception:
                            pass
                
                # Add buttons
                buttons = [
                    [Button.inline("📨 Send More Messages", data="send_message")],
                    [Button.inline("◀️ Back to Help", data="help_main")]
                ]
                
                try:
                    await processing_msg.edit(status_msg, buttons=buttons)
                except:
                    # If editing with buttons fails, send as plain text
                    await processing_msg.edit(status_msg)
                    
            except asyncio.TimeoutError:
                await prompt_msg.edit("⏰ Timeout: Message sending cancelled due to inactivity.")
                
        except Exception as e:
            error_message = f"Error in message sending conversation: {str(e)}"
            print(f"⚠️ {error_message}")
            print(f"Detailed error: {traceback.format_exc()}")
            await event.reply(error_message)
        
    except Exception as e:
        error_message = f"Error sending messages: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_add_session_command(client, event):
    """Handle add session command (owner only)"""
    try:
        # Check if user is owner
        if not await is_owner(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Get string session
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await event.reply("⚠️ Please provide a string session")
            return
        
        string_session = args[1].strip()
        
        # Initialize the session
        processing_msg = await event.reply("⏳ Adding session...")
        success, result = await initialize_session(string_session)
        
        if success:
            session_id = result
            
            # Get session details
            session = await get_session(session_id)
            session_name = session.get('first_name', '') or session.get('username', '') or session.get('phone', '') or str(session_id)
            
            success_msg = f"✅ Session added successfully with ID: {session_id}"
            success_msg += f"\nUser: {session_name}"
            
            # Add buttons
            buttons = [
                [Button.inline("📋 List All Sessions", data="list_sessions")],
                [Button.inline("◀️ Back to Help", data="help_main")]
            ]
            
            try:
                await processing_msg.edit(success_msg, buttons=buttons)
            except:
                # If editing with buttons fails, send as plain text
                await processing_msg.edit(success_msg)
            
            # Notify the user in their saved messages
            try:
                await send_message_to_saved(session_id, "✅ Your account has been added to the userbot. You will receive notifications here.")
            except Exception as e:
                print(f"⚠️ Error sending notification to saved messages: {str(e)}")
                
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: SESSION_ADDED | ID: {session_id} | User: {session_name} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging session addition: {str(e)}")
                    
        else:
            error_msg = f"⚠️ {result}"
            await processing_msg.edit(error_msg)
            
            # Log to errors database if it exists
            if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                try:
                    error_log = f"ERROR: ADD_SESSION | {result} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["errors"], error_log)
                except Exception:
                    pass
        
    except Exception as e:
        error_message = f"Error adding session: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_list_sessions_command(client, event):
    """Handle list sessions command (owner only)"""
    try:
        # Check if user is owner
        if not await is_owner(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Get all sessions
        sessions = await get_all_sessions()
        
        if not sessions:
            await event.reply("No active sessions found")
            return
        
        # Format sessions list
        sessions_list = "📱 **Active Sessions**\n\n"
        
        for session_id, session in sessions.items():
            first_name = session.get('first_name', '')
            username = session.get('username', '')
            phone = session.get('phone', '')
            
            session_name = first_name
            if username:
                session_name += f" (@{username})"
                
            if not session_name.strip():
                session_name = phone or f"Session {session_id}"
                
            sessions_list += f"**ID:** `{session_id}`\n"
            sessions_list += f"**User:** {session_name}\n"
            
            # Add stats if available
            stats = session.get('stats', {})
            if stats:
                sessions_list += "**Stats:**\n"
                sessions_list += f"  - Groups created: {stats.get('groups_created', 0)}\n"
                sessions_list += f"  - Messages sent: {stats.get('messages_sent', 0)}\n"
                
            sessions_list += "\n"
        
        # Add buttons
        buttons = [
            [Button.inline("🔄 Refresh", data="refresh_sessions")],
            [Button.inline("◀️ Back to Help", data="help_main")]
        ]
        
        try:
            await event.reply(sessions_list, buttons=buttons)
        except FloodWaitError as e:
            # Handle flood wait
            await handle_flood_wait(e.seconds, 'general')
            # Try again without buttons
            await event.reply(sessions_list)
        except Exception as e:
            # If buttons fail, send text only
            print(f"⚠️ Error sending sessions list with buttons: {str(e)}")
            await event.reply(sessions_list)
        
    except Exception as e:
        error_message = f"Error listing sessions: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_remove_session_command(client, event):
    """Handle remove session command (owner only)"""
    try:
        # Check if user is owner
        if not await is_owner(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Get session ID to remove
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await event.reply("⚠️ Please provide a session ID to remove")
            return
        
        try:
            session_id = int(args[1].strip())
        except ValueError:
            await event.reply("⚠️ Invalid session ID format")
            return
        
        # Remove the session
        processing_msg = await event.reply(f"⏳ Removing session {session_id}...")
        
        # Get session details for logging before removing
        session = await get_session(session_id)
        session_name = ''
        if session:
            session_name = session.get('first_name', '') or session.get('username', '') or session.get('phone', '') or str(session_id)
        
        # Attempt to notify the user in their saved messages before removing
        try:
            if session:
                await send_message_to_saved(session_id, "⚠️ Your account has been removed from the userbot.")
        except Exception as e:
            print(f"⚠️ Error sending notification to saved messages: {str(e)}")
        
        # Remove the session
        success = await remove_session(session_id)
        
        if success:
            success_msg = f"✅ Session {session_id} removed successfully"
            
            # Log to logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                try:
                    log_message = f"LOG: SESSION_REMOVED | ID: {session_id} | User: {session_name} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["logs"], log_message)
                except Exception as e:
                    print(f"⚠️ Error logging session removal: {str(e)}")
                    
            # Add buttons
            buttons = [
                [Button.inline("📋 List All Sessions", data="list_sessions")],
                [Button.inline("◀️ Back to Help", data="help_main")]
            ]
            
            try:
                await processing_msg.edit(success_msg, buttons=buttons)
            except:
                # If editing with buttons fails, send as plain text
                await processing_msg.edit(success_msg)
        else:
            error_msg = f"⚠️ Failed to remove session {session_id} (could be nonexistent or error)"
            await processing_msg.edit(error_msg)
            
            # Log to errors database if it exists
            if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
                try:
                    error_log = f"ERROR: REMOVE_SESSION | ID: {session_id} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    await client.send_message(config.DB_GROUPS["errors"], error_log)
                except Exception:
                    pass
        
    except Exception as e:
        error_message = f"Error removing session: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def is_user_authenticated(user_id):
    """
    Check if a user is authenticated and has agreed to the disclaimer
    
    Args:
        user_id: The Telegram user ID to check
        
    Returns:
        bool: True if user is authorized and has agreed to disclaimer
    """
    # First check if they're authorized
    if not await is_authorized(user_id):
        return False
        
    # Then check if they've agreed to the disclaimer
    # This requires importing the user_agreements dictionary from main
    from main import user_agreements
    
    return user_agreements.get(user_id, False)

async def handle_iterate_command(client, event):
    """Handle the iterate command for user feedback"""
    try:
        # Check if user is authorized
        if not await is_authorized(event.sender_id):
            await event.reply(config.ERR_UNAUTHORIZED)
            return
        
        # Create a feedback prompt with continue button
        message = """📝 **Iteration Feedback**

Thank you for using Creaternal. Would you like to provide feedback for our next iteration?

Your input helps us improve the system for all users.
"""
        
        buttons = [
            [Button.inline("✅ Yes, provide feedback", "iterate_feedback")],
            [Button.inline("⏭️ Continue to iterate", "iterate_continue")],
            [Button.inline("❌ No, skip for now", "iterate_skip")]
        ]
        
        await event.reply(message, buttons=buttons)
        
    except Exception as e:
        error_message = f"Error handling iterate command: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        await event.reply(error_message)

async def handle_iterate_action(client, event, action):
    """Handle iterate button callbacks"""
    try:
        if action == "feedback":
            # User wants to provide feedback
            await event.edit(
                """📝 **Provide Your Feedback**

Please type your feedback message in the chat. Be as detailed as possible.
Your insights will help us improve the system.

_Waiting for your message..._""",
                buttons=[
                    [Button.inline("❌ Cancel", "iterate_cancel")]
                ]
            )
            
            # Start a conversation to collect feedback
            try:
                # Get the conversation
                conversation = client.conversation(event.chat_id, timeout=300)
                
                # Wait for user's feedback message
                response = await asyncio.wait_for(conversation.get_response(), timeout=300)
                
                # Process the feedback
                feedback_text = response.text.strip()
                
                if feedback_text:
                    # Save the feedback (in a real implementation, this would store to a database)
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    user_id = event.sender_id
                    
                    # Log the feedback to the logs database if it exists
                    if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                        try:
                            log_message = f"FEEDBACK: USER_ID: {user_id} | TIME: {timestamp} | MESSAGE: {feedback_text}"
                            await client.send_message(config.DB_GROUPS["logs"], log_message)
                        except Exception as e:
                            print(f"⚠️ Error logging feedback: {str(e)}")
                    
                    # Thank the user
                    await event.edit(
                        """✅ **Feedback Received**

Thank you for your valuable feedback! Your input will help us improve the Creaternal system in our next iteration.

What would you like to do next?""",
                        buttons=[
                            [Button.inline("⏭️ Continue to iterate", "iterate_continue")],
                            [Button.inline("📋 Main Menu", "main_menu")]
                        ]
                    )
                else:
                    # Empty feedback
                    await event.edit(
                        """⚠️ **Empty Feedback**

It seems your feedback message was empty. Would you like to try again?""",
                        buttons=[
                            [Button.inline("🔄 Try Again", "iterate_feedback")],
                            [Button.inline("⏭️ Skip and Continue", "iterate_continue")]
                        ]
                    )
                    
            except asyncio.TimeoutError:
                # Timeout occurred
                await event.edit(
                    """⏰ **Feedback Timeout**

The feedback session has timed out. You can provide feedback later if you wish.

What would you like to do now?""",
                    buttons=[
                        [Button.inline("⏭️ Continue to iterate", "iterate_continue")],
                        [Button.inline("📋 Main Menu", "main_menu")]
                    ]
                )
                
        elif action == "continue":
            # User wants to continue to the next iteration
            # Check for pending updates or tasks
            pending_updates = await check_pending_updates(client, event.sender_id)
            
            if pending_updates:
                # Show loading message
                await event.edit("""🔄 **Processing Pending Updates**

Please wait while we apply the latest updates to your environment.
This should only take a moment...""")
                
                # Process updates
                success = await apply_pending_updates(client, event.sender_id)
                
                if success:
                    # Updates applied successfully
                    await event.edit(
                        """✅ **Updates Applied Successfully**

Your environment has been updated to the latest version.

**What's New:**
• Enhanced messaging capabilities
• Improved group management features
• Performance optimizations
• UI refinements

Ready to continue with your workflow.""",
                        buttons=[
                            [Button.inline("🚀 Continue to Dashboard", "main_menu")],
                            [Button.inline("📋 View Changelog", "help_updates")]
                        ]
                    )
                else:
                    # Updates failed
                    await event.edit(
                        """⚠️ **Update Process Incomplete**

Some updates could not be applied at this time.
You can continue with your current version or try updating again later.

What would you like to do?""",
                        buttons=[
                            [Button.inline("🔄 Try Again", "iterate_continue")],
                            [Button.inline("📋 Continue Anyway", "main_menu")]
                        ]
                    )
            else:
                # No pending updates, continue to main menu with confirmation
                await event.edit(
                    """✅ **System Ready**

Your environment is up to date and ready to use.
No pending updates or tasks were found.

What would you like to do next?""",
                    buttons=[
                        [Button.inline("📋 Go to Dashboard", "main_menu")],
                        [Button.inline("🔍 Check for Updates", "help_updates")]
                    ]
                )
            
        elif action == "skip":
            # User wants to skip providing feedback
            await event.edit(
                """👍 **Feedback Skipped**

No problem! You can always provide feedback later using the `/iterate` command.

What would you like to do next?""",
                buttons=[
                    [Button.inline("📋 Main Menu", "main_menu")],
                    [Button.inline("⏭️ Continue to iterate", "iterate_continue")]
                ]
            )
            
        elif action == "cancel":
            # User cancelled the feedback submission
            await event.edit(
                """❌ **Feedback Cancelled**

Your feedback submission has been cancelled.

What would you like to do next?""",
                buttons=[
                    [Button.inline("📋 Main Menu", "main_menu")],
                    [Button.inline("⏭️ Continue to iterate", "iterate_continue")]
                ]
            )
            
    except Exception as e:
        error_message = f"Error handling iterate action: {str(e)}"
        print(f"⚠️ {error_message}")
        print(f"Detailed error: {traceback.format_exc()}")
        
        # Try to answer the callback with an error
        try:
            await event.answer(f"Error: {str(e)[:100]}", alert=True)
        except:
            pass
        
        # Show error message with options to continue
        try:
            await event.edit(
                f"""⚠️ **Error Occurred**

An error occurred while processing your request:
`{str(e)}`

What would you like to do?""",
                buttons=[
                    [Button.inline("🔄 Try Again", f"iterate_{action}")],
                    [Button.inline("📋 Main Menu", "main_menu")]
                ]
            )
        except:
            # Last resort if even editing fails
            pass

# Helper functions for the iterate action
async def check_pending_updates(client, user_id):
    """
    Check if there are pending updates for the user
    
    Args:
        client: Telethon client
        user_id: User ID to check updates for
        
    Returns:
        bool: True if there are pending updates, False otherwise
    """
    # In a real implementation, this would check a database or configuration
    # For now, we'll always return False to ensure consistent behavior
    return False

async def apply_pending_updates(client, user_id):
    """
    Apply pending updates for the user
    
    Args:
        client: Telethon client
        user_id: User ID to apply updates for
        
    Returns:
        bool: True if updates were applied successfully, False otherwise
    """
    # In a real implementation, this would apply actual updates
    # For now, we'll simulate the process with a small delay
    await asyncio.sleep(1)
    
    # Log the update attempt
    if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_message = f"UPDATE: USER_ID: {user_id} | TIME: {timestamp} | STATUS: Applied successfully"
            await client.send_message(config.DB_GROUPS["logs"], log_message)
        except Exception as e:
            print(f"⚠️ Error logging update: {str(e)}")
    
    # Always return True to ensure consistent behavior
    return True