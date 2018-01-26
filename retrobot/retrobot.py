import os

from bot import Bot

SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
BOT_NAME = os.environ.get('BOT_NAME')
READ_WEBSOCKET_DELAY = 1 # 1 second delay between reading from firehose

def main():
    bot = Bot(SLACK_BOT_TOKEN, BOT_NAME, READ_WEBSOCKET_DELAY)
    bot.listen()

if __name__ == "__main__":
    main()
