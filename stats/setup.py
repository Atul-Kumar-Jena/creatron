"""
Setup module for Telegram userbot
Creates database groups and initializes configuration for the bot
"""
import asyncio
import time
import json
import traceback
import datetime
from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError

from utils.floodwait import handle_flood_wait
import config

async def create_database_groups(client):
    """
    Create database groups for storing bot data
    - Auth Users: Stores authorized user IDs
    - Sessions: Stores session data
    - Logs: Stores operation logs
    - Invites: Stores invite links
    - Stats: Stores usage statistics
    
    Args:
        client: Telethon client
    
    Returns:
        dict: Created database groups {name: group_id}
    """
    try:
        db_groups = {}
        print("🔄 Creating database groups...")
        
        # Define required database groups
        required_groups = {
            "auth_users": "Auth Users Database",
            "sessions": "Sessions Database",
            "logs": "Logs Database",
            "invites": "Invites Database",
            "stats": "Stats Database"
        }
        
        # Check if DB_GROUPS is already in config
        if hasattr(config, "DB_GROUPS") and config.DB_GROUPS:
            print("✅ DB_GROUPS found in config, using existing groups")
            return config.DB_GROUPS
        
        # Create groups with proper error handling
        for group_key, group_title in required_groups.items():
            try:
                print(f"🔄 Creating {group_title}...")
                
                # Create the supergroup
                result = await client(functions.channels.CreateChannelRequest(
                    title=group_title,
                    about=f"Database group for storing {group_key.replace('_', ' ')}",
                    megagroup=True
                ))
                
                # Get the channel ID
                channel = result.chats[0]
                channel_id = channel.id
                
                # Store in db_groups
                db_groups[group_key] = channel_id
                
                print(f"✅ Created {group_title} with ID {channel_id}")
                
                # Small delay to avoid flood wait
                await asyncio.sleep(2)
                
            except FloodWaitError as e:
                print(f"⚠️ FloodWaitError creating {group_title}: Wait for {e.seconds} seconds")
                await handle_flood_wait(e.seconds)
                
                # Try again after waiting
                try:
                    result = await client(functions.channels.CreateChannelRequest(
                        title=group_title,
                        about=f"Database group for storing {group_key.replace('_', ' ')}",
                        megagroup=True
                    ))
                    
                    # Get the channel ID
                    channel = result.chats[0]
                    channel_id = channel.id
                    
                    # Store in db_groups
                    db_groups[group_key] = channel_id
                    
                    print(f"✅ Created {group_title} with ID {channel_id} after FloodWait")
                    
                except Exception as retry_e:
                    print(f"⚠️ Error creating {group_title} after FloodWait: {str(retry_e)}")
                    traceback.print_exc()
                
            except Exception as e:
                print(f"⚠️ Error creating {group_title}: {str(e)}")
                traceback.print_exc()
        
        # Send welcome message to each group
        for group_key, group_id in db_groups.items():
            try:
                welcome_message = f"""📊 **{required_groups[group_key]} Initialized**

This group serves as a database for storing {group_key.replace('_', ' ')}.
**DO NOT DELETE THIS GROUP** as it's required for the bot to function properly.

Created on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
                await client.send_message(group_id, welcome_message)
                
            except Exception as e:
                print(f"⚠️ Error sending welcome message to {required_groups[group_key]}: {str(e)}")
        
        # Update config.py with the created database groups
        try:
            # Create DB_GROUPS dict in config module
            config.DB_GROUPS = db_groups
            
            # Also update the config.py file for persistence
            with open("config.py", "r") as f:
                config_content = f.read()
            
            # Check if DB_GROUPS is already in the file
            if "DB_GROUPS = " in config_content:
                # Replace existing DB_GROUPS
                lines = config_content.split("\n")
                for i, line in enumerate(lines):
                    if line.startswith("DB_GROUPS = "):
                        lines[i] = f"DB_GROUPS = {json.dumps(db_groups)}"
                        break
                        
                new_config_content = "\n".join(lines)
            else:
                # Add DB_GROUPS to the end of the file
                new_config_content = config_content + f"\n\n# Database groups for storing bot data\nDB_GROUPS = {json.dumps(db_groups)}\n"
            
            # Write the updated config
            with open("config.py", "w") as f:
                f.write(new_config_content)
                
            print("✅ Updated config.py with database groups")
            
        except Exception as e:
            print(f"⚠️ Error updating config.py with database groups: {str(e)}")
            traceback.print_exc()
            print("⚠️ You will need to manually add the DB_GROUPS to config.py")
            print(f"DB_GROUPS = {json.dumps(db_groups)}")
        
        print(f"✅ Database setup complete. Created {len(db_groups)} groups.")
        return db_groups
        
    except Exception as e:
        print(f"⚠️ Error in database setup: {str(e)}")
        traceback.print_exc()
        return {}

async def initialize_sessions():
    """
    Initialize all sessions from config.SESSIONS
    
    Returns:
        tuple: (success_count, failed_count, initialized_sessions)
    """
    try:
        # Check if SESSIONS exists in config
        if not hasattr(config, "SESSIONS") or not config.SESSIONS:
            print("⚠️ No sessions found in config. Please add sessions to config.SESSIONS.")
            return 0, 0, []
        
        # Import multi_session_manager to initialize sessions
        from modules.multi_session_manager import initialize_multi_sessions
        
        # Get all sessions from config
        sessions = config.SESSIONS
        
        # Initialize all sessions
        print(f"🔄 Initializing {len(sessions)} sessions from config...")
        success_count, failed_count, initialized_sessions = await initialize_multi_sessions(sessions)
        
        print(f"✅ Session initialization complete: {success_count} successful, {failed_count} failed")
        return success_count, failed_count, initialized_sessions
        
    except Exception as e:
        print(f"⚠️ Error initializing sessions: {str(e)}")
        traceback.print_exc()
        return 0, 0, []

async def add_sessions_to_config(sessions):
    """
    Add sessions to config.SESSIONS
    
    Args:
        sessions: List of string sessions to add
    
    Returns:
        bool: Success status
    """
    try:
        # Check if sessions is valid
        if not sessions or not isinstance(sessions, list):
            print("⚠️ Invalid sessions list")
            return False
        
        # Create or update SESSIONS in config module
        if not hasattr(config, "SESSIONS"):
            config.SESSIONS = []
        
        # Add new sessions (avoid duplicates)
        new_sessions = []
        for session in sessions:
            if session not in config.SESSIONS:
                config.SESSIONS.append(session)
                new_sessions.append(session)
        
        # Update config.py file for persistence
        try:
            with open("config.py", "r") as f:
                config_content = f.read()
            
            # Check if SESSIONS is already in the file
            if "SESSIONS = " in config_content:
                # Replace existing SESSIONS
                lines = config_content.split("\n")
                for i, line in enumerate(lines):
                    if line.startswith("SESSIONS = "):
                        lines[i] = f"SESSIONS = {json.dumps(config.SESSIONS)}"
                        break
                        
                new_config_content = "\n".join(lines)
            else:
                # Add SESSIONS to the end of the file
                new_config_content = config_content + f"\n\n# String sessions for multi-session operation\nSESSIONS = {json.dumps(config.SESSIONS)}\n"
            
            # Write the updated config
            with open("config.py", "w") as f:
                f.write(new_config_content)
                
            print(f"✅ Updated config.py with {len(new_sessions)} new sessions")
            
        except Exception as e:
            print(f"⚠️ Error updating config.py with sessions: {str(e)}")
            traceback.print_exc()
            print("⚠️ You will need to manually add the SESSIONS to config.py")
            print(f"SESSIONS = {json.dumps(config.SESSIONS)}")
        
        return True
        
    except Exception as e:
        print(f"⚠️ Error adding sessions to config: {str(e)}")
        traceback.print_exc()
        return False

async def setup_and_initialize(client):
    """
    Complete setup and initialization for the userbot
    
    Args:
        client: Telethon client
    
    Returns:
        bool: Success status
    """
    try:
        # Step 1: Create database groups
        print("🔄 Setting up database groups...")
        db_groups = await create_database_groups(client)
        
        if not db_groups:
            print("⚠️ Failed to create database groups")
            return False
        
        # Step 2: Initialize sessions
        print("🔄 Initializing sessions...")
        success_count, failed_count, initialized_sessions = await initialize_sessions()
        
        # Set up MESSAGE_COOLDOWN if not exists
        if not hasattr(config, "MESSAGE_COOLDOWN"):
            config.MESSAGE_COOLDOWN = 1.5  # Default 1.5 seconds between messages
            
            # Update config.py
            try:
                with open("config.py", "r") as f:
                    config_content = f.read()
                
                if "MESSAGE_COOLDOWN = " not in config_content:
                    # Add MESSAGE_COOLDOWN to the end of the file
                    new_config_content = config_content + f"\n\n# Cooldown between messages (seconds)\nMESSAGE_COOLDOWN = 1.5\n"
                    
                    # Write the updated config
                    with open("config.py", "w") as f:
                        f.write(new_config_content)
                        
                    print("✅ Added MESSAGE_COOLDOWN to config.py")
                
            except Exception as e:
                print(f"⚠️ Error adding MESSAGE_COOLDOWN to config.py: {str(e)}")
        
        # Log setup completion
        try:
            log_message = f"""✅ **Userbot Setup Complete**

Database Groups: {len(db_groups)}
Sessions Initialized: {success_count}/{success_count + failed_count}
Failed Sessions: {failed_count}

Setup Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            if "logs" in db_groups:
                await client.send_message(db_groups["logs"], log_message)
                
        except Exception as e:
            print(f"⚠️ Error logging setup completion: {str(e)}")
        
        print("✅ Setup and initialization complete!")
        return True
        
    except Exception as e:
        print(f"⚠️ Error in setup and initialization: {str(e)}")
        traceback.print_exc()
        return False