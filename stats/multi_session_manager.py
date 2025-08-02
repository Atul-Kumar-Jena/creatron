"""
Multi-Session Manager Module
Manages multiple Telegram sessions for creating and managing supergroups
"""
import asyncio
import time
import logging
import random
import json
import os
from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeExpiredError
from telethon.sessions import StringSession

from utils.floodwait import handle_flood_wait
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Dictionary to store active sessions
active_sessions = {}

async def create_session(api_id, api_hash, phone=None, string_session=None, password=None):
    """
    Create a new Telegram session
    
    Args:
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        phone: Phone number (required if string_session not provided)
        string_session: String session (optional)
        password: 2FA password (optional)
        
    Returns:
        tuple: (success_status, client or error_message)
    """
    try:
        logger.info(f"Creating new Telegram session...")
        
        # Check if we have either phone or string_session
        if not phone and not string_session:
            return False, "Either phone number or string session is required"
        
        # Create the client
        if string_session:
            client = TelegramClient(StringSession(string_session), api_id, api_hash)
        else:
            # Use memory session since we'll export to string later
            client = TelegramClient(StringSession(), api_id, api_hash)
        
        # Connect to Telegram
        await client.connect()
        
        # Already authorized?
        if await client.is_user_authorized():
            logger.info("Session already authorized")
            me = await client.get_me()
            session_id = me.id
            
            # Add to active sessions
            active_sessions[session_id] = {
                "client": client,
                "user_id": me.id,
                "username": me.username,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "created_at": time.time()
            }
            
            logger.info(f"Added session {session_id} to active sessions")
            
            return True, client
        
        # Need to authorize with phone
        if not string_session:
            logger.info(f"Sending code to {phone}")
            
            # Send code request
            await client.send_code_request(phone)
            
            # We need to return now and continue the login process with another function
            logger.info("Code sent, need verification to complete login")
            
            return False, "code_needed"
        
        # String session but not authorized (should not happen)
        return False, "Invalid string session"
    
    except FloodWaitError as e:
        logger.warning(f"FloodWaitError creating session: Wait for {e.seconds} seconds")
        await handle_flood_wait(e.seconds)
        
        # Try again after waiting
        try:
            # Create the client again
            if string_session:
                client = TelegramClient(StringSession(string_session), api_id, api_hash)
            else:
                client = TelegramClient(StringSession(), api_id, api_hash)
            
            # Connect to Telegram
            await client.connect()
            
            # Already authorized?
            if await client.is_user_authorized():
                logger.info("Session already authorized after FloodWait")
                me = await client.get_me()
                session_id = me.id
                
                # Add to active sessions
                active_sessions[session_id] = {
                    "client": client,
                    "user_id": me.id,
                    "username": me.username,
                    "phone": me.phone,
                    "first_name": me.first_name,
                    "last_name": me.last_name,
                    "created_at": time.time()
                }
                
                logger.info(f"Added session {session_id} to active sessions after FloodWait")
                
                return True, client
            
            # Need to authorize with phone
            if not string_session:
                logger.info(f"Sending code to {phone} after FloodWait")
                
                # Send code request
                await client.send_code_request(phone)
                
                # We need to return now and continue the login process with another function
                logger.info("Code sent after FloodWait, need verification to complete login")
                
                return False, "code_needed"
        
        except Exception as retry_e:
            logger.error(f"Error creating session on retry: {str(retry_e)}")
            return False, f"Error on retry: {str(retry_e)}"
    
    except Exception as e:
        error_message = f"Error creating session: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def complete_login(client, phone, code, password=None):
    """
    Complete login with verification code
    
    Args:
        client: Telethon client
        phone: Phone number
        code: Verification code
        password: 2FA password (optional)
        
    Returns:
        tuple: (success_status, client/string_session or error_message)
    """
    try:
        logger.info(f"Completing login for phone {phone}")
        
        # Sign in with code
        try:
            await client.sign_in(phone, code)
        except SessionPasswordNeededError:
            # 2FA enabled
            if not password:
                logger.warning("2FA password needed but not provided")
                return False, "2fa_needed"
            
            # Sign in with password
            await client.sign_in(password=password)
        
        # Get the string session for storage
        string_session = client.session.save()
        
        # Get user info
        me = await client.get_me()
        session_id = me.id
        
        # Add to active sessions
        active_sessions[session_id] = {
            "client": client,
            "user_id": me.id,
            "username": me.username,
            "phone": me.phone,
            "first_name": me.first_name,
            "last_name": me.last_name,
            "created_at": time.time(),
            "string_session": string_session
        }
        
        logger.info(f"Login completed successfully for {me.first_name} {me.last_name if me.last_name else ''}")
        
        return True, string_session
    
    except PhoneCodeExpiredError:
        logger.error("Verification code expired")
        return False, "code_expired"
    
    except FloodWaitError as e:
        logger.warning(f"FloodWaitError completing login: Wait for {e.seconds} seconds")
        await handle_flood_wait(e.seconds)
        
        # Try again after waiting
        try:
            # Sign in with code
            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                # 2FA enabled
                if not password:
                    logger.warning("2FA password needed but not provided after FloodWait")
                    return False, "2fa_needed"
                
                # Sign in with password
                await client.sign_in(password=password)
            
            # Get the string session for storage
            string_session = client.session.save()
            
            # Get user info
            me = await client.get_me()
            session_id = me.id
            
            # Add to active sessions
            active_sessions[session_id] = {
                "client": client,
                "user_id": me.id,
                "username": me.username,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "created_at": time.time(),
                "string_session": string_session
            }
            
            logger.info(f"Login completed successfully after FloodWait for {me.first_name} {me.last_name if me.last_name else ''}")
            
            return True, string_session
            
        except Exception as retry_e:
            logger.error(f"Error completing login on retry: {str(retry_e)}")
            return False, f"Error on retry: {str(retry_e)}"
    
    except Exception as e:
        error_message = f"Error completing login: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def load_session(string_session, api_id=None, api_hash=None):
    """
    Load a session from string
    
    Args:
        string_session: Session string
        api_id: API ID (optional, uses config if not provided)
        api_hash: API Hash (optional, uses config if not provided)
        
    Returns:
        tuple: (success_status, client or error_message)
    """
    try:
        # Use config values if not provided
        if not api_id:
            api_id = config.API_ID
        if not api_hash:
            api_hash = config.API_HASH
        
        logger.info(f"Loading session from string")
        
        # Create the client
        client = TelegramClient(StringSession(string_session), api_id, api_hash)
        
        # Connect to Telegram
        await client.connect()
        
        # Check if authorized
        if not await client.is_user_authorized():
            logger.error("Session not authorized")
            return False, "Session not authorized"
        
        # Get user info
        me = await client.get_me()
        session_id = me.id
        
        # Add to active sessions
        active_sessions[session_id] = {
            "client": client,
            "user_id": me.id,
            "username": me.username,
            "phone": me.phone,
            "first_name": me.first_name,
            "last_name": me.last_name,
            "created_at": time.time(),
            "string_session": string_session
        }
        
        logger.info(f"Session loaded successfully for {me.first_name} {me.last_name if me.last_name else ''}")
        
        return True, client
    
    except FloodWaitError as e:
        logger.warning(f"FloodWaitError loading session: Wait for {e.seconds} seconds")
        await handle_flood_wait(e.seconds)
        
        # Try again after waiting
        try:
            # Create the client
            client = TelegramClient(StringSession(string_session), api_id, api_hash)
            
            # Connect to Telegram
            await client.connect()
            
            # Check if authorized
            if not await client.is_user_authorized():
                logger.error("Session not authorized after FloodWait")
                return False, "Session not authorized"
            
            # Get user info
            me = await client.get_me()
            session_id = me.id
            
            # Add to active sessions
            active_sessions[session_id] = {
                "client": client,
                "user_id": me.id,
                "username": me.username,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "created_at": time.time(),
                "string_session": string_session
            }
            
            logger.info(f"Session loaded successfully after FloodWait for {me.first_name} {me.last_name if me.last_name else ''}")
            
            return True, client
            
        except Exception as retry_e:
            logger.error(f"Error loading session on retry: {str(retry_e)}")
            return False, f"Error on retry: {str(retry_e)}"
    
    except Exception as e:
        error_message = f"Error loading session: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def disconnect_session(session_id):
    """
    Disconnect a session and remove from active sessions
    
    Args:
        session_id: Session ID (user_id)
        
    Returns:
        bool: Success status
    """
    try:
        logger.info(f"Disconnecting session {session_id}")
        
        # Check if session exists
        if session_id not in active_sessions:
            logger.warning(f"Session {session_id} not found in active sessions")
            return False
        
        # Get the client
        client = active_sessions[session_id]["client"]
        
        # Disconnect
        await client.disconnect()
        
        # Remove from active sessions
        del active_sessions[session_id]
        
        logger.info(f"Session {session_id} disconnected and removed from active sessions")
        
        return True
    
    except Exception as e:
        error_message = f"Error disconnecting session: {str(e)}"
        logger.error(error_message)
        return False

async def disconnect_all_sessions():
    """
    Disconnect all active sessions
    
    Returns:
        tuple: (success_count, failed_count)
    """
    try:
        logger.info(f"Disconnecting all sessions")
        
        success_count = 0
        failed_count = 0
        
        # Copy session IDs to avoid modifying during iteration
        session_ids = list(active_sessions.keys())
        
        for session_id in session_ids:
            # Disconnect session
            success = await disconnect_session(session_id)
            
            if success:
                success_count += 1
            else:
                failed_count += 1
        
        logger.info(f"Disconnected {success_count} sessions, {failed_count} failed")
        
        return success_count, failed_count
    
    except Exception as e:
        error_message = f"Error disconnecting all sessions: {str(e)}"
        logger.error(error_message)
        return 0, len(active_sessions)

async def get_session_info(session_id):
    """
    Get session information
    
    Args:
        session_id: Session ID (user_id)
        
    Returns:
        tuple: (success_status, session_info or error_message)
    """
    try:
        logger.info(f"Getting info for session {session_id}")
        
        # Check if session exists
        if session_id not in active_sessions:
            logger.warning(f"Session {session_id} not found in active sessions")
            return False, "Session not found"
        
        # Get the session info
        session_info = active_sessions[session_id].copy()
        
        # Remove client object which can't be serialized
        if "client" in session_info:
            del session_info["client"]
        
        # Add active time
        session_info["active_time"] = time.time() - session_info["created_at"]
        
        logger.info(f"Got info for session {session_id}")
        
        return True, session_info
    
    except Exception as e:
        error_message = f"Error getting session info: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def get_all_sessions_info():
    """
    Get information for all active sessions
    
    Returns:
        list: List of session info dictionaries
    """
    try:
        logger.info(f"Getting info for all sessions")
        
        sessions_info = []
        
        for session_id in active_sessions:
            success, info = await get_session_info(session_id)
            
            if success:
                sessions_info.append(info)
        
        logger.info(f"Got info for {len(sessions_info)} sessions")
        
        return sessions_info
    
    except Exception as e:
        error_message = f"Error getting all sessions info: {str(e)}"
        logger.error(error_message)
        return []

async def save_sessions_to_file(filename="sessions.json"):
    """
    Save all sessions to a file
    
    Args:
        filename: Filename to save to
        
    Returns:
        tuple: (success_status, count or error_message)
    """
    try:
        logger.info(f"Saving sessions to file {filename}")
        
        # Get all sessions info
        sessions_info = await get_all_sessions_info()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        
        # Save to file
        with open(filename, "w") as f:
            json.dump(sessions_info, f)
        
        logger.info(f"Saved {len(sessions_info)} sessions to file {filename}")
        
        return True, len(sessions_info)
    
    except Exception as e:
        error_message = f"Error saving sessions to file: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def load_sessions_from_file(filename="sessions.json", api_id=None, api_hash=None):
    """
    Load sessions from a file
    
    Args:
        filename: Filename to load from
        api_id: API ID (optional, uses config if not provided)
        api_hash: API Hash (optional, uses config if not provided)
        
    Returns:
        tuple: (success_count, failed_count, error_message)
    """
    try:
        # Use config values if not provided
        if not api_id:
            api_id = config.API_ID
        if not api_hash:
            api_hash = config.API_HASH
        
        logger.info(f"Loading sessions from file {filename}")
        
        # Check if file exists
        if not os.path.isfile(filename):
            logger.error(f"File {filename} not found")
            return 0, 0, "File not found"
        
        # Load from file
        with open(filename, "r") as f:
            sessions_info = json.load(f)
        
        success_count = 0
        failed_count = 0
        
        # Load each session
        for session_info in sessions_info:
            # Check if session has string_session
            if "string_session" not in session_info:
                logger.warning(f"Session missing string_session, skipping")
                failed_count += 1
                continue
            
            # Load the session
            success, result = await load_session(
                session_info["string_session"],
                api_id,
                api_hash
            )
            
            if success:
                success_count += 1
            else:
                failed_count += 1
                logger.warning(f"Failed to load session: {result}")
        
        logger.info(f"Loaded {success_count} sessions from file {filename}, {failed_count} failed")
        
        return success_count, failed_count, None
    
    except Exception as e:
        error_message = f"Error loading sessions from file: {str(e)}"
        logger.error(error_message)
        return 0, 0, error_message

async def get_least_used_session():
    """
    Get the session with the least operations
    Useful for load balancing when making API calls
    
    Returns:
        tuple: (success_status, client or error_message)
    """
    try:
        logger.info(f"Getting least used session")
        
        # Check if any sessions are active
        if not active_sessions:
            logger.warning(f"No active sessions found")
            return False, "No active sessions"
        
        # Simple implementation: randomize for now
        # In a real implementation, we would track operations per session
        session_id = random.choice(list(active_sessions.keys()))
        
        logger.info(f"Selected session {session_id} as least used")
        
        return True, active_sessions[session_id]["client"]
    
    except Exception as e:
        error_message = f"Error getting least used session: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def execute_with_session(session_id, func, *args, **kwargs):
    """
    Execute a function with a specific session
    
    Args:
        session_id: Session ID (user_id)
        func: Function to execute
        *args, **kwargs: Arguments to pass to the function
        
    Returns:
        The result of the function call
    """
    try:
        logger.info(f"Executing function with session {session_id}")
        
        # Check if session exists
        if session_id not in active_sessions:
            logger.warning(f"Session {session_id} not found in active sessions")
            return False, "Session not found"
        
        # Get the client
        client = active_sessions[session_id]["client"]
        
        # Execute the function
        result = await func(client, *args, **kwargs)
        
        logger.info(f"Function executed with session {session_id}")
        
        return result
    
    except Exception as e:
        error_message = f"Error executing function with session: {str(e)}"
        logger.error(error_message)
        return False, error_message

async def execute_with_all_sessions(func, *args, **kwargs):
    """
    Execute a function with all active sessions
    
    Args:
        func: Function to execute
        *args, **kwargs: Arguments to pass to the function
        
    Returns:
        dict: Dictionary of session_id -> result
    """
    try:
        logger.info(f"Executing function with all sessions")
        
        results = {}
        
        # Execute with each session
        for session_id in active_sessions:
            # Get the client
            client = active_sessions[session_id]["client"]
            
            # Execute the function
            try:
                result = await func(client, *args, **kwargs)
                results[session_id] = result
            except Exception as e:
                logger.error(f"Error executing function with session {session_id}: {str(e)}")
                results[session_id] = (False, f"Error: {str(e)}")
        
        logger.info(f"Function executed with {len(results)} sessions")
        
        return results
    
    except Exception as e:
        error_message = f"Error executing function with all sessions: {str(e)}"
        logger.error(error_message)
        return {}

async def execute_with_random_session(func, *args, **kwargs):
    """
    Execute a function with a random session
    
    Args:
        func: Function to execute
        *args, **kwargs: Arguments to pass to the function
        
    Returns:
        The result of the function call
    """
    try:
        # Get least used session
        success, result = await get_least_used_session()
        
        if not success:
            return False, result
        
        client = result
        
        # Execute the function
        result = await func(client, *args, **kwargs)
        
        return result
    
    except Exception as e:
        error_message = f"Error executing function with random session: {str(e)}"
        logger.error(error_message)
        return False, error_message