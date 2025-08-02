"""
Configuration file for Telegram Multi-Group Creator Bot
Version: 5.0
Last Updated: August 1, 2025
"""

# Telegram API credentials
API_ID = 25121584
API_HASH = 'd462bdebcee798a6cf4e21727a17436c'
OWNER_ID = 803243487  # Owner's Telegram ID

# Bot token - Replace with your actual bot token from @BotFather
BOT_TOKEN = '7650781586:AAHP6D7_43d97emjJrv7-WrrRvE-01c9vBk'  # Replace with your actual bot token

# Group for storing sessions - Set this to your group's ID
# This group will be used as a database to store user sessions
SESSION_STORAGE_GROUP_ID = -1002733158915  # Replace with your actual group ID

# Group for storing summaries and statistics
# This group will be used to track created groups and allow querying
SUMMARY_GROUP_ID = -1002868040359  # Replace with your actual summary group ID

# Rate limiting settings
MAX_RETRY_ATTEMPTS = 5         # Maximum number of retry attempts after FloodWait
DAILY_GROUP_LIMIT = 50         # Maximum number of groups to create per session per day
GROUPS_PER_SESSION = 50        # Number of groups to create per session
GROUP_CREATION_DELAY = 7       # Delay between creating groups (seconds) - significantly reduced
BATCH_SIZE = 10                # Number of groups to create before taking a break - increased for efficiency
BATCH_COOLDOWN = 60            # Cooldown between batches (seconds) - reduced significantly
SESSION_SWITCH_DELAY = 30      # Delay when switching to a new session (seconds) - reduced significantly

# Error handling settings
MAX_ERRORS_PER_SESSION = 10    # Maximum consecutive errors before skipping a session
ERROR_COOLDOWN = 60            # Cooldown after encountering errors (seconds) - reduced