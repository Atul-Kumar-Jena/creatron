"""
Session Manager
Handles validation and management of Telegram sessions
"""

import asyncio
import json
import logging
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import AuthKeyUnregisteredError, FloodWaitError

# Configure logger
logger = logging.getLogger(__name__)

async def check_session_valid(session_string, api_id, api_hash):
    """
    Check if a session string is valid
    
    Args:
        session_string: The session string to check
        api_id: Telegram API ID
        api_hash: Telegram API Hash
    
    Returns:
        bool: True if the session is valid, False otherwise
    """
    try:
        # Create a client with the provided session string
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        
        # Connect to Telegram
        await client.connect()
        
        # Check if the client is authorized
        is_authorized = await client.is_user_authorized()
        
        # Disconnect the client
        await client.disconnect()
        
        return is_authorized
    
    except AuthKeyUnregisteredError:
        # This exception is raised when the session is not valid
        return False
    
    except FloodWaitError:
        # If we hit a FloodWaitError, we'll consider the session valid but rate-limited
        return True
    
    except Exception as e:
        # Any other exception indicates an invalid session
        print(f"Error checking session: {str(e)}")
        return False

async def store_session(client, group_id, user_id, session_string, description=None):
    """
    Store a session string in a storage group
    
    Args:
        client: Telethon client
        group_id: ID of the storage group
        user_id: User ID associated with the session
        session_string: Session string to store
        description: Optional description of the session
    
    Returns:
        bool: True if the session was stored successfully, False otherwise
    """
    try:
        # Format the session storage message
        message = f"SESSION_DATA\n{{\n"
        message += f"  \"user_id\": {user_id},\n"
        message += f"  \"session\": \"{session_string}\",\n"
        
        if description:
            message += f"  \"description\": \"{description}\",\n"
        
        message += f"  \"stored_at\": \"{asyncio.get_event_loop().time()}\"\n"
        message += "}"
        
        # Send the message to the storage group
        await client.send_message(group_id, message)
        
        logger.info(f"Stored session for user {user_id} in storage group")
        return True
    
    except Exception as e:
        logger.error(f"Error storing session: {str(e)}")
        return False

async def load_sessions(client, group_id, limit=100):
    """
    Load sessions from a storage group
    
    Args:
        client: Telethon client
        group_id: ID of the storage group
        limit: Maximum number of messages to retrieve
    
    Returns:
        dict: Dictionary of user_id -> [session_strings]
    """
    try:
        # Get messages from the storage group
        messages = await client.get_messages(group_id, limit=limit)
        
        # Parse sessions from messages
        sessions = {}
        
        for message in messages:
            if not message.text:
                continue
            
            if message.text.startswith("SESSION_DATA"):
                try:
                    # Extract the JSON part
                    json_text = message.text[len("SESSION_DATA"):].strip()
                    
                    # Parse the JSON
                    session_data = json.loads(json_text)
                    
                    # Extract user_id and session
                    user_id = session_data.get("user_id")
                    session = session_data.get("session")
                    
                    if user_id and session:
                        # Initialize the user's session list if needed
                        if user_id not in sessions:
                            sessions[user_id] = []
                        
                        # Add this session to the user's list
                        sessions[user_id].append(session)
                
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse session data from message: {message.id}")
                    continue
                
                except Exception as e:
                    logger.warning(f"Error processing session message: {str(e)}")
                    continue
        
        logger.info(f"Loaded {sum(len(s) for s in sessions.values())} sessions for {len(sessions)} users")
        return sessions
    
    except Exception as e:
        logger.error(f"Error loading sessions: {str(e)}")
        return {}