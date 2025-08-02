"""
Authentication and authorization utilities for the Telegram userbot
"""
import asyncio
import re
from telethon import events, utils
from telethon.tl.functions.messages import ImportChatInviteRequest, CreateChatRequest
from telethon.tl.functions.channels import CreateChannelRequest, JoinChannelRequest
from telethon.errors import UserAlreadyParticipantError, InviteHashInvalidError, ChannelInvalidError

import config
import datetime

# Cache of authorized users
authorized_users = set()
is_initialized = False

async def initialize_auth(client):
    """
    Initialize the authentication system by joining or creating the database group
    and loading authorized users
    """
    global is_initialized
    
    if is_initialized:
        return True
    
    try:
        # Try to access the auth database group
        db_auth_group_id = None
        
        # Check if we have a DB_GROUPS structure with an auth group
        if hasattr(config, "DB_GROUPS") and "auth" in config.DB_GROUPS and config.DB_GROUPS["auth"]:
            db_auth_group_id = config.DB_GROUPS["auth"]
            
        # Otherwise, use the legacy DB_GROUP_ID for backwards compatibility
        elif hasattr(config, "DB_GROUP_ID") and config.DB_GROUP_ID:
            db_auth_group_id = config.DB_GROUP_ID
        
        # If we have an auth group, try to access it
        if db_auth_group_id:
            try:
                # Try to convert string ID to int if needed
                if isinstance(db_auth_group_id, str) and db_auth_group_id.startswith("-100"):
                    try:
                        db_auth_group_id = int(db_auth_group_id)
                    except ValueError:
                        print(f"⚠️ Could not convert auth group ID to integer: {db_auth_group_id}")
                
                print(f"🔄 Attempting to access auth database group with ID: {db_auth_group_id}")
                entity = await client.get_entity(db_auth_group_id)
                print(f"✅ Successfully accessed auth database group: {entity.title}")
                
                # Store the ID in config.DB_GROUPS if it doesn't exist
                if not hasattr(config, "DB_GROUPS"):
                    config.DB_GROUPS = {}
                config.DB_GROUPS["auth"] = db_auth_group_id
                
                # Load authorized users from this group
                await load_authorized_users_from_group(client, db_auth_group_id)
                
            except Exception as e:
                print(f"⚠️ Could not access auth database group by ID: {str(e)}")
                # Will try other methods
        
        # Try to use the setup module to create database structure if it's available
        # and we don't have an auth group yet
        if not db_auth_group_id or not authorized_users:
            try:
                # Import the setup module only if we need it
                from modules.setup import create_database_structure
                
                print("🔄 No valid auth database group found. Creating database structure...")
                db_groups_info = await create_database_structure(client)
                
                if db_groups_info and "auth" in db_groups_info:
                    print(f"✅ Created auth database group: {db_groups_info['auth']['title']}")
                    
                    # Load authorized users again from the new group
                    await load_authorized_users_from_group(client, db_groups_info["auth"]["id"])
            except ImportError:
                print("⚠️ Setup module not available, cannot create database structure automatically")
            except Exception as e:
                print(f"⚠️ Error creating database structure: {str(e)}")
        
        # Always ensure owner is authorized
        if hasattr(config, "OWNER_ID") and config.OWNER_ID:
            owner_id = config.OWNER_ID
            if isinstance(owner_id, str):
                try:
                    owner_id = int(owner_id)
                    config.OWNER_ID = owner_id  # Store the integer version
                except ValueError:
                    print(f"⚠️ Could not convert owner ID to integer: {owner_id}")
                    
            authorized_users.add(owner_id)
            
            # Add owner to auth database if we have one and they're not already there
            if hasattr(config, "DB_GROUPS") and "auth" in config.DB_GROUPS and config.DB_GROUPS["auth"]:
                try:
                    await add_user_to_auth_db(client, owner_id, is_owner=True)
                except Exception as e:
                    print(f"⚠️ Error adding owner to auth database: {str(e)}")
        
        # Log clarification about owner vs session user
        try:
            # Get information about the current session user
            me = await client.get_me()
            current_user_id = me.id
            
            # Check if the current user is the owner
            if hasattr(config, "OWNER_ID") and config.OWNER_ID != current_user_id:
                print(f"ℹ️ NOTE: Current session user (ID: {current_user_id}) is different from configured owner (ID: {config.OWNER_ID})")
                print(f"ℹ️ The configured owner has full control of the bot, while the session user is the account running the bot")
                
                # Ensure the session user is also authorized
                authorized_users.add(current_user_id)
                
                # Add to auth database
                if hasattr(config, "DB_GROUPS") and "auth" in config.DB_GROUPS and config.DB_GROUPS["auth"]:
                    try:
                        await add_user_to_auth_db(client, current_user_id, is_session_user=True)
                    except Exception as e:
                        print(f"⚠️ Error adding session user to auth database: {str(e)}")
                        
                # Log to the logs database if it exists
                if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                    try:
                        log_message = f"LOG: SESSION_USER_DIFFERENT | Owner ID: {config.OWNER_ID} | Session User ID: {current_user_id} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        await client.send_message(config.DB_GROUPS["logs"], log_message)
                    except Exception as e:
                        print(f"⚠️ Error logging session user info: {str(e)}")
                        
        except Exception as e:
            print(f"⚠️ Error clarifying owner vs session user: {str(e)}")
        
        is_initialized = True
        return True
        
    except Exception as e:
        print(f"⚠️ Error initializing auth: {str(e)}")
        # Even on error, ensure owner is authorized in memory
        if hasattr(config, "OWNER_ID") and config.OWNER_ID:
            owner_id = config.OWNER_ID
            if isinstance(owner_id, str):
                try:
                    owner_id = int(owner_id)
                except ValueError:
                    pass
            authorized_users.add(owner_id)
        return False

async def load_authorized_users_from_group(client, group_id):
    """
    Load authorized users from a specific group
    """
    global authorized_users
    
    try:
        print(f"🔄 Loading authorized users from group ID: {group_id}")
        
        # Clear the current cache (except owner)
        owner_id = None
        if hasattr(config, "OWNER_ID") and config.OWNER_ID:
            owner_id = config.OWNER_ID
            if isinstance(owner_id, str):
                try:
                    owner_id = int(owner_id)
                except ValueError:
                    pass
        
        authorized_users = {owner_id} if owner_id else set()
        
        # Pattern to match auth entries: "AUTH: user_id"
        auth_pattern = re.compile(r"AUTH: (\d+)")
        
        # Search for auth messages in the group
        async for message in client.iter_messages(group_id, search="AUTH:", limit=200):
            if message and message.text:
                for match in auth_pattern.finditer(message.text):
                    user_id = int(match.group(1))
                    authorized_users.add(user_id)
        
        # Remove users that have UNAUTH entries
        unauth_pattern = re.compile(r"UNAUTH: (\d+)")
        unauth_users = set()
        
        async for message in client.iter_messages(group_id, search="UNAUTH:", limit=200):
            if message and message.text:
                for match in unauth_pattern.finditer(message.text):
                    user_id = int(match.group(1))
                    unauth_users.add(user_id)
        
        # Remove any users that have been unauthorized
        for user_id in unauth_users:
            if user_id in authorized_users and user_id != owner_id:  # Never remove owner
                authorized_users.remove(user_id)
        
        print(f"✅ Loaded {len(authorized_users)} authorized users")
        print(f"✅ Authorized users: {authorized_users}")
        
        # Log to the logs database if it exists
        if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
            try:
                log_message = f"LOG: AUTH_USERS_LOADED | Count: {len(authorized_users)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["logs"], log_message)
            except Exception as e:
                print(f"⚠️ Error logging auth users load: {str(e)}")
                
    except Exception as e:
        print(f"⚠️ Error loading authorized users from group: {str(e)}")
        # Ensure owner is still authorized
        if hasattr(config, "OWNER_ID") and config.OWNER_ID:
            owner_id = config.OWNER_ID
            if isinstance(owner_id, str):
                try:
                    owner_id = int(owner_id)
                except ValueError:
                    pass
            authorized_users.add(owner_id)

async def is_authorized(user_id):
    """
    Check if a user is authorized
    """
    # Convert user_id to int if it's a string
    if isinstance(user_id, str):
        try:
            user_id = int(user_id)
        except ValueError:
            return False
    
    # Convert owner_id to int if it's a string
    if hasattr(config, "OWNER_ID") and config.OWNER_ID:
        owner_id = config.OWNER_ID
        if isinstance(owner_id, str):
            try:
                owner_id = int(owner_id)
            except ValueError:
                pass  # Keep as string if can't convert
        
        # Owner is always authorized, regardless of database
        if user_id == owner_id:
            return True
    
    return user_id in authorized_users

async def is_owner(user_id):
    """
    Check if a user is the owner
    """
    # Convert user_id to int if it's a string
    if isinstance(user_id, str):
        try:
            user_id = int(user_id)
        except ValueError:
            return False
    
    # Convert owner_id to int if it's a string
    if hasattr(config, "OWNER_ID") and config.OWNER_ID:
        owner_id = config.OWNER_ID
        if isinstance(owner_id, str):
            try:
                owner_id = int(owner_id)
            except ValueError:
                pass  # Keep as string if can't convert
        
        return user_id == owner_id
    
    return False

async def add_user_to_auth_db(client, user_id, is_owner=False, is_session_user=False):
    """
    Add a user to the auth database
    
    Args:
        client: Telethon client
        user_id: User ID to add
        is_owner: Whether this user is the owner
        is_session_user: Whether this user is the session user
    """
    # Get the auth database group ID
    auth_group_id = None
    if hasattr(config, "DB_GROUPS") and "auth" in config.DB_GROUPS and config.DB_GROUPS["auth"]:
        auth_group_id = config.DB_GROUPS["auth"]
    elif hasattr(config, "DB_GROUP_ID") and config.DB_GROUP_ID:
        auth_group_id = config.DB_GROUP_ID
    
    if not auth_group_id:
        print("⚠️ No auth database group configured")
        return False
    
    # Check if user is already in the auth database
    auth_pattern = f"AUTH: {user_id}"
    user_already_in_db = False
    
    try:
        async for message in client.iter_messages(auth_group_id, search=auth_pattern, limit=5):
            if message and message.text and auth_pattern in message.text:
                user_already_in_db = True
                break
        
        if not user_already_in_db:
            # Add auth entry to database group
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Add role information
            user_role = ""
            if is_owner:
                user_role = " | Role: OWNER"
            elif is_session_user:
                user_role = " | Role: SESSION_USER"
                
            auth_message = f"AUTH: {user_id} | Added: {timestamp}{user_role}"
            await client.send_message(auth_group_id, auth_message)
            
            # Log to the logs database if it exists
            if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
                role_info = ""
                if is_owner:
                    role_info = " (OWNER)"
                elif is_session_user:
                    role_info = " (SESSION_USER)"
                    
                log_message = f"LOG: USER_AUTHORIZED | User ID: {user_id}{role_info} | {timestamp}"
                await client.send_message(config.DB_GROUPS["logs"], log_message)
            
            return True
        else:
            print(f"User {user_id} already in auth database")
            return True
            
    except Exception as e:
        print(f"⚠️ Error adding user to auth database: {str(e)}")
        
        # Log to the errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_message = f"ERROR: ADD_USER_AUTH | User ID: {user_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_message)
            except Exception:
                pass
                
        return False

async def remove_user_from_auth_db(client, user_id):
    """
    Remove a user from the auth database by deleting their AUTH message
    """
    # Get the auth database group ID
    auth_group_id = None
    if hasattr(config, "DB_GROUPS") and "auth" in config.DB_GROUPS and config.DB_GROUPS["auth"]:
        auth_group_id = config.DB_GROUPS["auth"]
    elif hasattr(config, "DB_GROUP_ID") and config.DB_GROUP_ID:
        auth_group_id = config.DB_GROUP_ID
    
    if not auth_group_id:
        print("⚠️ No auth database group configured")
        return False
    
    # Find and delete all AUTH messages for this user
    auth_pattern = f"AUTH: {user_id}"
    messages_deleted = 0
    
    try:
        async for message in client.iter_messages(auth_group_id, search=auth_pattern, limit=10):
            if message and message.text and auth_pattern in message.text:
                await message.delete()
                messages_deleted += 1
        
        # Add UNAUTH marker in the database
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        unauth_message = f"UNAUTH: {user_id} | Removed: {timestamp}"
        await client.send_message(auth_group_id, unauth_message)
        
        # Log to the logs database if it exists
        if hasattr(config, "DB_GROUPS") and "logs" in config.DB_GROUPS and config.DB_GROUPS["logs"]:
            log_message = f"LOG: USER_UNAUTHORIZED | User ID: {user_id} | Messages Deleted: {messages_deleted} | {timestamp}"
            await client.send_message(config.DB_GROUPS["logs"], log_message)
        
        return messages_deleted > 0
            
    except Exception as e:
        print(f"⚠️ Error removing user from auth database: {str(e)}")
        
        # Log to the errors database if it exists
        if hasattr(config, "DB_GROUPS") and "errors" in config.DB_GROUPS and config.DB_GROUPS["errors"]:
            try:
                error_message = f"ERROR: REMOVE_USER_AUTH | User ID: {user_id} | {str(e)} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                await client.send_message(config.DB_GROUPS["errors"], error_message)
            except Exception:
                pass
                
        return False

async def authorize_user(client, user_id):
    """
    Authorize a user
    """
    try:
        # Convert user_id to int if it's a string
        if isinstance(user_id, str):
            try:
                user_id = int(user_id)
            except ValueError:
                print(f"⚠️ Invalid user ID format: {user_id}")
                return False
        
        # Check if already authorized in memory
        if user_id in authorized_users:
            return True
        
        # Add to auth database
        success = await add_user_to_auth_db(client, user_id)
        
        # Add to local cache regardless of database result
        authorized_users.add(user_id)
        
        return True
        
    except Exception as e:
        print(f"⚠️ Error authorizing user: {str(e)}")
        return False

async def unauthorize_user(client, user_id):
    """
    Unauthorize a user
    """
    try:
        # Convert user_id to int if it's a string
        if isinstance(user_id, str):
            try:
                user_id = int(user_id)
            except ValueError:
                print(f"⚠️ Invalid user ID format: {user_id}")
                return False
            
        # Can't unauthorize the owner
        if await is_owner(user_id):
            print(f"⚠️ Cannot unauthorize the owner (ID: {user_id})")
            return False
        
        # Remove from auth database
        await remove_user_from_auth_db(client, user_id)
        
        # Remove from local cache
        if user_id in authorized_users:
            authorized_users.remove(user_id)
        
        return True
        
    except Exception as e:
        print(f"⚠️ Error unauthorizing user: {str(e)}")
        return False

async def get_authorized_users():
    """
    Get all authorized users
    """
    return list(authorized_users)

async def load_authorized_users(client):
    """
    Load authorized users (maintained for backwards compatibility)
    """
    try:
        # Try to use DB_GROUPS["auth"] first
        if hasattr(config, "DB_GROUPS") and "auth" in config.DB_GROUPS and config.DB_GROUPS["auth"]:
            await load_authorized_users_from_group(client, config.DB_GROUPS["auth"])
        # Fall back to legacy DB_GROUP_ID
        elif hasattr(config, "DB_GROUP_ID") and config.DB_GROUP_ID:
            await load_authorized_users_from_group(client, config.DB_GROUP_ID)
        else:
            # Default to owner only
            if hasattr(config, "OWNER_ID") and config.OWNER_ID:
                owner_id = config.OWNER_ID
                if isinstance(owner_id, str):
                    try:
                        owner_id = int(owner_id)
                    except ValueError:
                        pass
                global authorized_users
                authorized_users = {owner_id}
    except Exception as e:
        print(f"⚠️ Error loading authorized users: {str(e)}")
        print(f"Detailed error: {traceback.format_exc()}")