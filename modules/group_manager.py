"""
Group Manager module for the Telegram userbot
Provides essential functions for creating and managing supergroups
"""
import asyncio
import datetime
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest

async def create_supergroup(client, title, about=None):
    """
    Create a new supergroup
    
    Args:
        client: Telethon client
        title: Group title
        about: Group description (optional)
    
    Returns:
        tuple: (success_status, group_entity or error_message)
    """
    try:
        print(f"🔄 Creating supergroup '{title}'...")
        
        # Use default description if not provided
        if not about:
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            about = f"Supergroup created by userbot on {current_time}"
        
        # Create the supergroup
        result = await client(CreateChannelRequest(
            title=title,
            about=about,
            megagroup=True  # Set to True for supergroup
        ))
        
        # Get the created channel entity
        channel = result.chats[0]
        
        print(f"✅ Successfully created supergroup '{title}' (ID: {channel.id})")
        return (True, channel)
    
    except FloodWaitError as e:
        wait_time = e.seconds
        print(f"⚠️ FloodWaitError: Need to wait {wait_time} seconds")
        return (False, f"FloodWaitError: Need to wait {wait_time} seconds")
    
    except Exception as e:
        error_msg = f"Error creating supergroup: {str(e)}"
        print(f"❌ {error_msg}")
        return (False, error_msg)

async def generate_invite_link(client, chat_id):
    """
    Generate an invite link for a supergroup
    
    Args:
        client: Telethon client
        chat_id: Chat ID or entity
    
    Returns:
        tuple: (success_status, invite_link or error_message)
    """
    try:
        print(f"🔄 Generating invite link for group...")
        
        # Export chat invite link
        invite = await client(ExportChatInviteRequest(
            peer=chat_id
        ))
        
        print(f"✅ Successfully generated invite link")
        return (True, invite.link)
    
    except FloodWaitError as e:
        wait_time = e.seconds
        print(f"⚠️ FloodWaitError: Need to wait {wait_time} seconds")
        return (False, f"FloodWaitError: Need to wait {wait_time} seconds")
    
    except Exception as e:
        error_msg = f"Error generating invite link: {str(e)}"
        print(f"❌ {error_msg}")
        return (False, error_msg)