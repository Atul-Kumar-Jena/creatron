"""
Telegram Bot
A streamlined Telegram bot for creating supergroups using multiple user sessions
"""

from telethon import TelegramClient, events
import logging
import asyncio
import sys
import os
import threading
from flask import Flask
from modules.multi_commands import handle_message, load_sessions_from_storage_group
from config import API_ID, API_HASH, BOT_TOKEN, SESSION_STORAGE_GROUP_ID, SUMMARY_GROUP_ID

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize the client as a bot with bot token
client = TelegramClient('creaternal_bot', API_ID, API_HASH)

# Create Flask app to keep the service alive on Render
app = Flask(__name__)

@app.route('/')
def home():
    """Simple endpoint to confirm the bot is running"""
    return "Telegram Bot is running!"

@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy", "bot": "running"}

# Register event handler for bot commands and messages
@client.on(events.NewMessage())
async def message_handler(event):
    """Handle all incoming messages"""
    try:
        # Make sure to use the handle_message from multi_commands
        await handle_message(client, event)
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}")
        # Try to respond with an error message if possible
        try:
            await event.respond(f"❌ An error occurred: {str(e)}\n\nPlease try again or contact the bot administrator.")
        except:
            pass

async def initialize_bot_database():
    """Send initial messages to database groups if needed"""
    try:
        # Check and initialize storage group
        try:
            await client.send_message(
                SESSION_STORAGE_GROUP_ID,
                "🔄 Bot restarted - Session storage group connected!"
            )
            logger.info("Successfully connected to session storage group")
        except Exception as e:
            logger.error(f"Could not connect to session storage group: {str(e)}")
        
        # Check and initialize summary group
        try:
            await client.send_message(
                SUMMARY_GROUP_ID,
                "📊 Bot restarted - Summary database group connected!\n\n"
                "This group stores summaries of all created groups and allows querying.\n\n"
                "Available commands in this group:\n"
                "/stats - View overall statistics\n"
                "/user_[ID] - View groups created by a specific user\n"
                "/search [query] - Search for groups by name"
            )
            logger.info("Successfully connected to summary group")
        except Exception as e:
            logger.error(f"Could not connect to summary group: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error initializing database groups: {str(e)}")

async def startup_tasks():
    """Initialize tasks when the bot starts"""
    try:
        # Verify and initialize database groups
        await initialize_bot_database()
        
        # Verify the storage group exists and the bot has access
        try:
            entity = await client.get_entity(SESSION_STORAGE_GROUP_ID)
            logger.info(f"Successfully connected to storage group: {entity.title if hasattr(entity, 'title') else entity.id}")
        except Exception as e:
            logger.error(f"Cannot access storage group: {str(e)}")
            logger.warning("Bot will run without a storage group. Some functionality may be limited.")
        
        # Verify the summary group exists and the bot has access
        try:
            entity = await client.get_entity(SUMMARY_GROUP_ID)
            logger.info(f"Successfully connected to summary group: {entity.title if hasattr(entity, 'title') else entity.id}")
        except Exception as e:
            logger.error(f"Cannot access summary group: {str(e)}")
            logger.warning("Bot will run without a summary group. Some functionality may be limited.")
        
        # Load existing sessions from the storage group
        try:
            await load_sessions_from_storage_group(client)
        except Exception as e:
            logger.error(f"Failed to load sessions: {str(e)}")
            logger.info("Bot will continue without loading previous sessions")
        
        logger.info("Bot startup tasks completed")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")

def run_flask():
    """Run the Flask web server on a separate thread"""
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# Main execution block
if __name__ == "__main__":
    print("Starting Telegram Multi-Group Creator Bot...")
    
    try:
        # Start the web server in a separate thread
        flask_thread = threading.Thread(target=run_flask)
        flask_thread.daemon = True
        flask_thread.start()
        logger.info(f"Flask web server started on port {os.environ.get('PORT', 8080)}")
        
        # Start the client with bot token
        client.start(bot_token=BOT_TOKEN)
        
        # Run startup tasks
        client.loop.run_until_complete(startup_tasks())
        
        print("Bot started successfully! Press Ctrl+C to exit.")
        print("Available commands:")
        print("/start - Start the bot")
        print("/creategroups - Start the group creation process")
        print("/summary - View your group creation summary")
        print("/help - Get detailed help")
        
        # Run the client until disconnected
        client.run_until_disconnected()
    except KeyboardInterrupt:
        print("Bot stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"Critical error starting bot: {str(e)}")
        logging.error(f"Critical startup error: {str(e)}")
        sys.exit(1)
